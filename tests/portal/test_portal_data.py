"""FinGraph Portal: SQL dựng an toàn và logic gợi ý baseline (không cần AWS, không cần streamlit)."""

import pytest

from apps.portal import data as D


def test_sql_builders_reject_injection():
    for bad in ["2 OR 1=1", "2; DROP TABLE x", "abc", "", "-1"]:
        with pytest.raises(ValueError):
            D.sql_client_latest(bad)
        with pytest.raises(ValueError):
            D.sql_customer_uid(bad)


def test_sql_builders_use_int_and_expected_tables():
    s = D.sql_client_latest(" 42 ")
    assert "client_id = 42" in s and "track_a_gold.berka_client_month" in s
    assert "natural_key = 'client_id=42'" in D.sql_customer_uid(42)


def test_segment_rates_escapes_region_and_uses_train_only():
    s = D.sql_segment_rates("north ' OR '1'='1", 47.9)
    assert "north '' OR ''1''=''1" in s           # dấu nháy được nhân đôi
    assert "= 4" in s                               # nhóm tuổi 40-49
    assert s.count("split = 'train'") == 2          # không dùng nhãn tập test


PROFILE = {"has_loan": "true", "has_card_classic": "false", "has_card_junior": "false",
           "has_card_gold": "false", "has_insurance": "false", "has_household": "true"}


def test_recommend_skips_owned_and_ranks_by_segment_rate():
    rates = [
        {"product": "loan", "rate_seg": "0.9", "rate_all": "0.1", "n": "500"},          # đã có -> bỏ
        {"product": "card_classic", "rate_seg": "0.05", "rate_all": "0.02", "n": "500"},
        {"product": "insurance", "rate_seg": "0.08", "rate_all": "0.04", "n": "500"},
        {"product": "household", "rate_seg": "0.5", "rate_all": "0.3", "n": "500"},     # đã có -> bỏ
        {"product": "unknown", "rate_seg": "1", "rate_all": "1", "n": "500"},          # ngoài danh mục
    ]
    recs = D.recommend(PROFILE, rates)
    assert [r["product"] for r in recs] == ["insurance", "card_classic"]
    assert recs[0]["lift"] == pytest.approx(2.0)
    assert recs[0]["basis"] == "nhóm tương tự"


def test_recommend_small_segment_falls_back_to_overall_rate():
    rates = [{"product": "card_gold", "rate_seg": "0.9", "rate_all": "0.01", "n": "5"},
             {"product": "insurance", "rate_seg": "0.03", "rate_all": "0.03", "n": "400"}]
    recs = D.recommend(PROFILE, rates, min_n=30)
    gold = next(r for r in recs if r["product"] == "card_gold")
    assert gold["score"] == pytest.approx(0.01)      # không tin nhóm 5 dòng
    assert gold["basis"].startswith("toàn bộ")
    assert recs[0]["product"] == "insurance"


def test_explain_error_messages():
    m = D.explain_error("COLUMN_NOT_FOUND: line 1:8: Column 'full_name' cannot be resolved or requester "
                        "is not authorized to access requested resources")
    assert "full_name" in m and "che" in m
    assert "không được đọc" in D.explain_error("Insufficient permissions to execute the query.")


def test_personas_profiles_overridable(monkeypatch):
    monkeypatch.setenv("FINGRAPH_PROFILE_ANALYST", "x-analyst")
    p = D.personas()
    assert p["analyst"].profile == "x-analyst"
    assert p["de"].graph and not p["analyst"].graph and p["customer"].customer
    assert p["customer"].workgroup.endswith("customer-client-2")


def test_readonly_sql():
    assert D.readonly_sql("SELECT * FROM silver_corebank.client") .endswith("LIMIT 1000")
    assert D.readonly_sql("with a as (select 1) select * from a limit 5;").endswith("limit 5")
    assert D.readonly_sql("SELECT 'drop table x' AS s").startswith("SELECT")   # từ khoá trong chuỗi: hợp lệ
    for bad in ["DROP TABLE x", "SELECT 1; DROP TABLE x", "INSERT INTO a SELECT 1",
                "WITH a AS (SELECT 1) DELETE FROM b", "", "show tables"]:
        with pytest.raises(ValueError):
            D.readonly_sql(bad)
