"""Backtest rule gian lận thẻ (transforms/gold/card_fraud_eval.py) trên Spark + Iceberg thật."""

from __future__ import annotations

import datetime as dt

from tests.silver.conftest import requires_spark

pytestmark = requires_spark


def _tx(n, card, ts, cat, amt):
    t = dt.datetime.fromisoformat(ts).replace(tzinfo=dt.timezone.utc)
    return (n, card, cat, t, t.date(), amt)


class Lake:
    """Thay table()/write_table() bang DataFrame trong bo nho: test chay ca tren Windows (khong can Hadoop native)."""

    def __init__(self, spark):
        self.spark, self.t = spark, {}

    def table(self, spark, db, name):
        return self.t[f"{db}.{name}"]

    def write_table(self, df, db, name, partition_by=None, comment=""):
        self.t[f"{db}.{name}"] = df.localCheckpoint()
        return self.t[f"{db}.{name}"].count()


def _seed(spark, lake):
    rows = [_tx(f"h{i}", "A", f"2026-02-{i:02d} 12:00:00", "grocery_pos", 50.0) for i in range(1, 26)]
    rows += [
        _tx("a1", "A", "2026-03-01 01:00:00", "shopping_net", 400.0),
        _tx("a2", "A", "2026-03-01 01:10:00", "shopping_net", 420.0),
        _tx("a3", "A", "2026-03-01 01:20:00", "misc_net", 380.0),
        _tx("a4", "A", "2026-03-01 01:30:00", "grocery_pos", 390.0),
        _tx("a5", "A", "2026-03-01 15:00:00", "grocery_pos", 40.0),
        _tx("b1", "B", "2026-03-01 10:00:00", "travel", 300.0),
        # SAU ngay chup: khong duoc lot vao baseline (ro du lieu tuong lai)
        _tx("f1", "A", "2026-03-01 20:00:00", "grocery_pos", 9000.0),
    ]
    fraud = {"a1", "a2", "a3", "a4"}
    lab = [(r[0], r[0] in fraud, f"labels_{r[4]:%Y%m%d}.csv.gz") for r in rows]
    # Luong KHONG nhan (producer khac) tren cung the A, ngay truoc a1: neu lot vao, a3 da du 4 giao dich / 2 gio
    rows += [_tx(f"u{i}", "A", f"2026-03-01 00:5{i}:00", "travel", 5.0) for i in range(3)]
    lake.t["silver_cards.card_authorization"] = spark.createDataFrame(
        rows, "trans_num string, card_token string, category string, trans_ts timestamp, txn_date date, amount double")
    lake.t["silver_cards.chargeback"] = spark.createDataFrame(
        lab, "trans_num string, is_fraud boolean, label_file string")


def test_backtest_rule_va_chi_so(spark, monkeypatch):
    from transforms.gold import card_fraud_eval as E

    lake = Lake(spark)
    monkeypatch.setattr(E, "table", lake.table)
    monkeypatch.setattr(E, "write_table", lake.write_table)
    _seed(spark, lake)
    out = E.run(spark, {"EVAL_FROM": "2026-03-01", "EVAL_TO": "2026-03-01"})
    # 7 giao dich co nhan trong ngay; u0..u2 khong nhan khong vao danh gia, khong lam tang n_2h
    assert out["card_rule_eval_txn"] == 7

    tx = {r["trans_num"]: r for r in lake.t["track_a_gold.card_rule_eval_txn"].collect()}
    a1 = tx["a1"]
    assert a1["basis"] == "the" and a1["b_n"] == 25 and a1["amt_threshold"] == 125.0
    assert str(a1["baseline_as_of"]) == "2026-02-28"
    assert a1["r_amount_spike"] and a1["r_amount_spike_high"] and a1["r_night_high_risk_high"]
    assert not tx["a3"]["r_velocity_2h"] and tx["a4"]["r_velocity_2h"] and tx["a4"]["n_2h"] == 4
    assert tx["a4"]["multi_rule_3"]
    assert tx["b1"]["basis"] == "mac dinh" and not tx["b1"]["any_rule"]
    assert not tx["a5"]["any_rule"]
    # f1 (9000 CZK) bi bat theo baseline the, chung to no KHONG lam phong baseline cua chinh ngay do
    assert tx["f1"]["r_amount_spike"]

    s = {(r["level"], r["rule"], r["severity"]): r
         for r in lake.t["track_a_gold.card_rule_eval_summary"].collect()}
    amt = s[("txn", "AMOUNT_SPIKE", "any")]
    assert (amt["n"], amt["n_fraud"], amt["tp"], amt["fp"], amt["fn"]) == (7, 4, 4, 1, 0)
    assert amt["recall"] == 1.0 and abs(amt["precision"] - 0.8) < 1e-9
    vel = s[("txn", "VELOCITY_2H", "any")]
    assert (vel["tp"], vel["fn"], vel["recall"]) == (1, 3, 0.25)
    cd = s[("card_day", "ANY_RULE", "any")]
    assert (cd["n"], cd["tp"], cd["fp"], cd["tn"]) == (2, 1, 0, 1)
    daily = lake.t["track_a_gold.card_rule_eval_daily"].collect()
    assert {r["rule"] for r in daily} >= {"AMOUNT_SPIKE", "NIGHT_HIGH_RISK", "VELOCITY_2H", "MULTI_RULE_2"}
