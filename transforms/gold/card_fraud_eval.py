"""GOLD — đánh giá rule gian lận thẻ realtime bằng nhãn chargeback (precision / recall).

Vì sao làm trên Gold chứ không trên ClickHouse: nhãn đến trễ 20 ngày (silver_cards.chargeback), còn ClickHouse chỉ
giữ giao dịch 14 ngày và cảnh báo 30 ngày. Lake giữ đủ cả giao dịch lẫn nhãn, nên đánh giá bằng cách CHẠY LẠI
(backtest) đúng ba rule của ClickHouse (repo hạ tầng modules/kafka/realtime/clickhouse/sql/30_rules.sql) trên lịch sử
silver_cards.card_authorization, rồi so với nhãn.

Baseline ĐÚNG THỜI ĐIỂM, không rò dữ liệu tương lai: lưới ngày chụp s = EVAL_FROM + k * REFRESH_DAYS. Giao dịch ngày d
dùng bản chụp s gần nhất <= d, tính trên giao dịch ngày [s - BASELINE_DAYS, s - 1]: đúng như gold.card_baseline chạy
vào ngày s (as_of = s - 1). Realtime nạp baseline mỗi giờ; REFRESH_DAYS = 7 là xấp xỉ rẻ (đặt 1 để khớp hằng ngày).

Khác realtime (ghi vào luận văn): VELOCITY_2H ở đây xét MỌI giao dịch, còn realtime chỉ xét 3 giờ gần nhất (tồn đọng cũ
hơn không được chấm). Backtest vì vậy đo LOGIC rule, không đo sự cố vận hành của luồng realtime.

Ba bảng, ghi đè mỗi lần:
  card_rule_eval_txn      (trans_num)                 từng giao dịch đã có nhãn: cờ từng rule, ngưỡng, nhãn
  card_rule_eval_summary  (level, rule, severity)     TP/FP/FN/TN, precision, recall, F1, tỉ lệ cảnh báo
  card_rule_eval_daily    (txn_date, rule)            như trên theo ngày (dashboard)
level = txn (mỗi giao dịch) | card_day (thẻ x ngày: dương nếu có giao dịch gian lận, bắt được nếu có cảnh báo).

UNIVERSE (mặc định labeled): tập giao dịch dùng cho baseline, velocity và đánh giá.
  labeled  chỉ giao dịch CÓ dòng nhãn. Trên dev, landing chứa nhiều lần chạy producer độc lập chồng lên cùng 919 thẻ
           core banking; chỉ một lần chạy có file nhãn (LabelSink giữ nhãn trong bộ nhớ, producer khởi động lại là
           mất). Giao dịch của lần chạy khác không nhãn mà vẫn có gian lận, trộn vào thì velocity / baseline của thẻ
           bị nhiễu bởi gian lận không ai chấm được. Đo 11-18: 150.340 giao dịch có nhãn, 600.960 không nhãn.
           Lọc theo SỰ CÓ MẶT của dòng nhãn (thuộc luồng nào), không theo giá trị nhãn: không rò nhãn vào rule.
  all      mọi giao dịch (đúng hành vi production, khi mọi giao dịch rồi sẽ có nhãn).

Tham số (--KHOA): UNIVERSE, CARDS_DB (silver_cards), GOLD_DB (track_a_gold), BASELINE_DAYS (90), REFRESH_DAYS (7),
WARMUP_DAYS (30: EVAL_FROM mặc định = ngày giao dịch đầu tiên + WARMUP_DAYS), EVAL_FROM, EVAL_TO (YYYY-MM-DD, tuỳ chọn).
"""

from __future__ import annotations

from typing import Dict, List

from ..common import current, table, write_table

NIGHT_HOURS = 5
NIGHT_CATEGORIES = ("shopping_net", "misc_net", "grocery_pos")
MIN_CARD_TXN = 20
VELOCITY_WINDOW_S = 7200
RULES = ("AMOUNT_SPIKE", "NIGHT_HIGH_RISK", "VELOCITY_2H")
# Tổng hợp như rt.v_card_risk: thẻ dính >= 2 rule khác nhau trong 2 giờ (2 = high, 3 = critical)
COMBINED = ("ANY_RULE", "MULTI_RULE_2", "MULTI_RULE_3")


def _baselines(F, tx, eval_from: str, n_snap: int, refresh: int, days: int):
    """(card_bl, cat_bl) theo (snap_k, khoá). Mỗi giao dịch lịch sử ngày t góp vào bản chụp s khi
    s - days <= t <= s - 1, tức k trong [ceil((t + 1 - from) / R), floor((t + days - from) / R)]."""
    off = F.datediff(F.col("txn_date"), F.to_date(F.lit(eval_from)))
    k_lo = F.greatest(F.ceil((off + 1) / refresh), F.lit(0)).cast("int")
    k_hi = F.least(F.floor((off + days) / refresh), F.lit(n_snap - 1)).cast("int")
    hist = (tx.select("card_token", "category", "amt", "is_night", k_lo.alias("k_lo"), k_hi.alias("k_hi"))
            .where(F.col("k_lo") <= F.col("k_hi"))
            .withColumn("snap_k", F.explode(F.sequence("k_lo", "k_hi"))))
    q = F.percentile_approx("amt", [0.5, 0.9, 0.99], 1000)
    card = (hist.groupBy("snap_k", "card_token")
            .agg(F.count("*").alias("b_n"), q.alias("_q"), F.avg("is_night").alias("b_night"))
            .select("snap_k", "card_token", "b_n", F.col("_q")[0].alias("b_p50"), F.col("_q")[1].alias("b_p90"),
                    "b_night"))
    cat = (hist.groupBy("snap_k", "category").agg(q.alias("_q"))
           .select("snap_k", "category", F.col("_q")[1].alias("c_p90"), F.col("_q")[2].alias("c_p99")))
    return card, cat


def rule_columns(F):
    """Cột cờ rule, cùng ngưỡng với 30_rules.sql. Cần: amt, category, hour, b_n, b_p90, b_night, c_p90, c_p99, n_2h."""
    b_n = F.coalesce(F.col("b_n"), F.lit(0))
    c_p90 = F.coalesce(F.col("c_p90"), F.lit(0.0))
    c_p99 = F.coalesce(F.col("c_p99"), F.lit(0.0))
    basis = F.when(b_n >= MIN_CARD_TXN, "the").when(c_p99 > 0, "nhom").otherwise("mac dinh")
    thr = (F.when(b_n >= MIN_CARD_TXN, F.greatest(F.col("b_p90") * 2.5, F.lit(100.0)))
           .when(c_p99 > 0, F.greatest(c_p99, F.lit(100.0))).otherwise(F.lit(500.0)))
    night_thr = F.when(c_p90 > 0, c_p90).otherwise(F.lit(150.0))
    amount = F.col("amt") > thr
    night = (F.col("hour") < NIGHT_HOURS) & F.col("category").isin(*NIGHT_CATEGORIES) & (F.col("amt") > night_thr)
    velocity = F.col("n_2h") >= 4
    return {
        "basis": basis, "amt_threshold": thr, "night_threshold": night_thr,
        "r_amount_spike": amount,
        "r_amount_spike_high": amount & (F.col("amt") >= thr * 2),
        "r_night_high_risk": night,
        "r_night_high_risk_high": night & (b_n >= MIN_CARD_TXN) & (F.coalesce(F.col("b_night"), F.lit(1.0)) < 0.05),
        "r_velocity_2h": velocity,
        "r_velocity_2h_high": F.col("n_2h") >= 6,
    }


RULE_FLAG = {"AMOUNT_SPIKE": "r_amount_spike", "NIGHT_HIGH_RISK": "r_night_high_risk", "VELOCITY_2H": "r_velocity_2h"}


def _metrics(F, df, keys: List[str], pred: str, label: str = "is_fraud"):
    p, y = F.col(pred).cast("boolean"), F.col(label).cast("boolean")
    s = lambda c: F.sum(F.when(c, 1).otherwise(0))  # noqa: E731
    agg = df.groupBy(*keys).agg(F.count("*").alias("n"), s(y).alias("n_fraud"), s(p & y).alias("tp"),
                                s(p & ~y).alias("fp"), s(~p & y).alias("fn"), s(~p & ~y).alias("tn"))
    prec = F.when(F.col("tp") + F.col("fp") > 0, F.col("tp") / (F.col("tp") + F.col("fp")))
    rec = F.when(F.col("n_fraud") > 0, F.col("tp") / F.col("n_fraud"))
    return (agg.withColumn("precision", prec).withColumn("recall", rec)
            .withColumn("f1", F.when(F.col("precision") + F.col("recall") > 0,
                                     2 * F.col("precision") * F.col("recall") / (F.col("precision") + F.col("recall"))))
            .withColumn("alert_rate", (F.col("tp") + F.col("fp")) / F.col("n")))


def _long(F, df, extra: List[str]):
    """Mỗi (rule, severity) một dòng với cột pred: để tính chỉ số bằng một groupBy."""
    parts = []
    for rule in RULES:
        flag = RULE_FLAG[rule]
        parts.append(df.select(*extra, F.lit(rule).alias("rule"), F.lit("any").alias("severity"),
                               F.col(flag).alias("pred")))
        parts.append(df.select(*extra, F.lit(rule).alias("rule"), F.lit("high").alias("severity"),
                               F.col(flag + "_high").alias("pred")))
    for rule, col in zip(COMBINED, ("any_rule", "multi_rule_2", "multi_rule_3")):
        parts.append(df.select(*extra, F.lit(rule).alias("rule"), F.lit("any").alias("severity"),
                               F.col(col).alias("pred")))
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out


def run(spark, args: Dict[str, str]) -> Dict[str, int]:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    src = args.get("CARDS_DB", "silver_cards")
    gold = args.get("GOLD_DB", "track_a_gold")
    days = int(args.get("BASELINE_DAYS", "90"))
    refresh = max(1, int(args.get("REFRESH_DAYS", "7")))
    warmup = int(args.get("WARMUP_DAYS", "30"))
    universe = args.get("UNIVERSE", "labeled")
    if universe not in ("labeled", "all"):
        raise ValueError(f"UNIVERSE phai la labeled | all, nhan {universe!r}")

    labels = (current(table(spark, src, "chargeback"), "trans_num")
              .select("trans_num", F.col("is_fraud").cast("boolean").alias("is_fraud"), "label_file"))
    tx = current(table(spark, src, "card_authorization"), "trans_num")
    if universe == "labeled":
        tx = tx.join(labels.select("trans_num"), "trans_num", "left_semi")
    tx = (tx.select("trans_num", "card_token", "category", "trans_ts", "txn_date",
                  F.col("amount").cast("double").alias("amt"),
                  F.hour("trans_ts").alias("hour"),
                  (F.hour("trans_ts") < NIGHT_HOURS).cast("int").alias("is_night"))
          .cache())
    first_txn, last_txn = tx.agg(F.min("txn_date"), F.max("txn_date")).first()
    if first_txn is None:
        print(f"  {src}.card_authorization rong -> bo qua")
        return {}
    lab_dates = (tx.join(labels, "trans_num").agg(F.min("txn_date"), F.max("txn_date")).first())
    if lab_dates[0] is None:
        print("  chua co giao dich nao co nhan -> bo qua")
        return {}
    import datetime as dt

    eval_from = args.get("EVAL_FROM") or str(max(lab_dates[0], first_txn + dt.timedelta(days=warmup)))
    eval_to = args.get("EVAL_TO") or str(lab_dates[1])
    n_snap = (dt.date.fromisoformat(eval_to) - dt.date.fromisoformat(eval_from)).days // refresh + 1
    print(f"  giao dich {first_txn}..{last_txn}; co nhan {lab_dates[0]}..{lab_dates[1]}; "
          f"tap {universe}; danh gia {eval_from}..{eval_to}, baseline {days} ngay, chup moi {refresh} ngay ({n_snap} ban)")
    if n_snap <= 0:
        print("  cua so danh gia rong -> bo qua")
        return {}

    card_bl, cat_bl = _baselines(F, tx, eval_from, n_snap, refresh, days)

    # VELOCITY: dem tren MOI giao dich (ke ca chua co nhan) roi moi loc cua so danh gia
    w2h = Window.partitionBy("card_token").orderBy(F.col("trans_ts").cast("long")).rangeBetween(-VELOCITY_WINDOW_S, 0)
    vel = tx.select("trans_num", F.count("*").over(w2h).alias("n_2h"))

    off = F.datediff(F.col("txn_date"), F.to_date(F.lit(eval_from)))
    ev = (tx.where((F.col("txn_date") >= F.lit(eval_from)) & (F.col("txn_date") <= F.lit(eval_to)))
          .join(labels, "trans_num")
          .join(vel, "trans_num")
          .withColumn("snap_k", F.floor(off / refresh).cast("int"))
          .join(card_bl, ["snap_k", "card_token"], "left")
          .join(cat_bl, ["snap_k", "category"], "left")
          .withColumn("baseline_as_of",
                      F.date_sub(F.date_add(F.to_date(F.lit(eval_from)), F.col("snap_k") * refresh), 1)))
    for name, expr in rule_columns(F).items():
        ev = ev.withColumn(name, expr)
    # Tong hop: so rule KHAC NHAU the dinh trong 2 gio toi giao dich nay (tinh tren tap danh gia)
    flags = [RULE_FLAG[r] for r in RULES]
    n_rules = sum(F.max(F.col(f).cast("int")).over(w2h) for f in flags)
    ev = (ev.withColumn("n_rules_2h", n_rules)
          .withColumn("any_rule", F.greatest(*[F.col(f) for f in flags]))
          .withColumn("multi_rule_2", F.col("n_rules_2h") >= 2)
          .withColumn("multi_rule_3", F.col("n_rules_2h") >= 3)
          .withColumn("eval_from", F.to_date(F.lit(eval_from))).withColumn("eval_to", F.to_date(F.lit(eval_to)))
          .withColumn("baseline_days", F.lit(days)).withColumn("refresh_days", F.lit(refresh))
          .withColumn("universe", F.lit(universe))
          .drop("snap_k", "is_night")
          .cache())

    preds = [*flags, *[f + "_high" for f in flags], "any_rule", "multi_rule_2", "multi_rule_3"]
    long_txn = _long(F, ev, ["txn_date", "is_fraud"])
    card_day = (ev.groupBy("card_token", "txn_date")
                .agg(F.max(F.col("is_fraud").cast("int")).cast("boolean").alias("is_fraud"),
                     *[F.max(F.col(p).cast("int")).cast("boolean").alias(p) for p in preds]))
    long_card = _long(F, card_day, ["txn_date", "is_fraud"])

    keys = ["rule", "severity"]
    summary = (_metrics(F, long_txn, keys, "pred").withColumn("level", F.lit("txn"))
               .unionByName(_metrics(F, long_card, keys, "pred").withColumn("level", F.lit("card_day")))
               .withColumn("eval_from", F.to_date(F.lit(eval_from))).withColumn("eval_to", F.to_date(F.lit(eval_to)))
               .withColumn("baseline_days", F.lit(days)).withColumn("refresh_days", F.lit(refresh))
               .withColumn("universe", F.lit(universe)))
    daily = (_metrics(F, long_txn.where(F.col("severity") == "any"), ["txn_date", "rule"], "pred")
             .withColumn("level", F.lit("txn")))

    n_txn = write_table(ev, gold, "card_rule_eval_txn",
                        comment="Backtest rule gian lan the tren giao dich da co nhan chargeback (tung giao dich)")
    n_sum = write_table(summary, gold, "card_rule_eval_summary",
                        comment="Precision/recall rule gian lan the theo rule x muc do x cap (txn | card_day)")
    n_day = write_table(daily, gold, "card_rule_eval_daily",
                        comment="Precision/recall rule gian lan the theo ngay (cap giao dich)")
    for r in summary.where(F.col("severity") == "any").orderBy("level", "rule").collect():
        print(f"  {r['level']:9s} {r['rule']:16s} n={r['n']} fraud={r['n_fraud']} tp={r['tp']} fp={r['fp']} "
              f"P={r['precision']} R={r['recall']}")
    ev.unpersist()
    tx.unpersist()
    return {"card_rule_eval_txn": n_txn, "card_rule_eval_summary": n_sum, "card_rule_eval_daily": n_day}
