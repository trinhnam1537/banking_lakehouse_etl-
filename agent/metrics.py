"""Tầng chỉ số (semantic / metrics layer) cho agent: thống kê TỔNG HỢP có định nghĩa sẵn, có trích dẫn.

Vì sao cần (đánh giá lần 1, 18-11, câu A07, A11):
  - Câu thống kê ("tỉ lệ đăng ký theo nghề nghiệp") không có uid/triple để trích, nên kiểm grounding chặn câu trả
    lời dù số đúng; agent phải tự viết SQL qua tool `query` (8-12 lượt, vượt số lượt tối đa).
  - Tool `query` chỉ đọc track_a_onto/track_a_gold; dữ liệu thống kê lại nằm ở silver.

Cách làm: mỗi chỉ số là một định nghĩa CỐ ĐỊNH (bảng nguồn, độ đo, các chiều được phép, bộ lọc được phép).
LLM chỉ chọn `metric` + `by` + `filters`; SQL sinh từ định nghĩa, tham số đi qua whitelist, không ghép chuỗi tự do.
Mỗi kết quả trả `cite_id` = băm (chỉ số, chiều, bộ lọc) 32 hex: agent trích [metric:<cite_id>] và nêu `source_tables`.
Kiểm grounding coi cite_id như một id đã thấy trong phiên (agent/grounding.py).

Mọi bảng silver đọc qua `_cur()`: bản ghi HIỆN HÀNH (bỏ dòng xoá, lấy bản mới nhất theo ds_key), cùng quy tắc với
transforms.common.current. Chạy dưới persona analyst: Lake Formation che cột PII; các chỉ số ở đây không cần cột PII.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


class MetricError(ValueError):
    """Tham số sai: trả cho LLM để tự sửa (giống ToolError)."""


def _cur(table: str) -> str:
    """Bản hiện hành của một bảng silver (append-only, scd4a)."""
    return (f"(SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY ds_key ORDER BY ds_etl_timestamp DESC) AS _rn "
            f"FROM {table}) WHERE _rn = 1 AND coalesce(ds_record_status, 'I') <> 'D')")


@dataclass
class Metric:
    name: str
    description: str
    source_tables: List[str]
    base: str                                   # FROM ... (đã có alias, có thể là sub-select)
    measures: Dict[str, str]                    # tên cột kết quả -> biểu thức tổng hợp
    dims: Dict[str, str]                        # tên chiều -> biểu thức trên base
    filters: Dict[str, str] = field(default_factory=dict)   # tên bộ lọc -> biểu thức so bằng giá trị
    order: Optional[str] = None                 # ORDER BY mặc định (tên cột kết quả)
    note: str = ""
    aggregate: bool = True                      # False: bảng đã tổng hợp sẵn, trả từng dòng (mọi chiều luôn có mặt)
    default_filters: Dict[str, Any] = field(default_factory=dict)


BM = ("(SELECT job, marital, education, age, contact_channel AS contact, last_contact_month AS month, CAST(subscribed AS integer) AS sub, "
      "'full' AS dataset FROM " + _cur("silver_bank_marketing.contact_full") + " UNION ALL "
      "SELECT job, marital, education, age, contact_channel, last_contact_month, CAST(subscribed AS integer), 'additional' FROM "
      + _cur("silver_bank_marketing.contact_additional") + ") t")

MBD = ("(SELECT client_id, count(*) AS n_months, max(CAST(target_1 AS integer)) AS b1, "
       "max(CAST(target_2 AS integer)) AS b2, max(CAST(target_3 AS integer)) AS b3, max(CAST(target_4 AS integer)) AS b4 "
       "FROM " + _cur("silver_mbd.target_month") + " GROUP BY client_id) t")

BERKA_LOAN = ("(SELECT l.loan_id, l.amount, l.duration_months, l.loan_status, l.is_problem_loan, l.granted_date, "
              "x.region, x.district_name FROM " + _cur("silver_berka.loan") + " l JOIN " + _cur("silver_berka.account")
              + " a ON a.account_id = l.account_id JOIN " + _cur("silver_berka.district")
              + " x ON x.district_id = a.district_id) t")

BERKA_CLIENT = ("(SELECT c.client_id, c.gender, x.region, x.district_name FROM " + _cur("silver_berka.client")
                + " c JOIN " + _cur("silver_berka.district") + " x ON x.district_id = c.district_id) t")

AML_ATTEMPT = ("(SELECT attempt_id, typology, txn_id FROM " + _cur("silver_aml.laundering_attempt") + ") t")

PAYSIM = ("(SELECT x.txn_type, CAST(f.is_fraud AS integer) AS fraud, CAST(f.is_flagged_fraud AS integer) AS flagged, "
          "x.amount FROM " + _cur("silver_paysim.transaction") + " x JOIN " + _cur("silver_paysim.fraud_label")
          + " f ON f.txn_ref = x.txn_ref) t")

METRICS: Dict[str, Metric] = {m.name: m for m in [
    Metric("bank_marketing_subscription",
           "Chiến dịch Bank Marketing (UCI, 2 bộ full + additional): số liên hệ, số đăng ký tiền gửi có kỳ hạn, tỉ lệ "
           "đăng ký. Theo nghề nghiệp, hôn nhân, học vấn, nhóm tuổi, kênh liên hệ, tháng, bộ dữ liệu.",
           ["silver_bank_marketing.contact_full", "silver_bank_marketing.contact_additional"], BM,
           {"n_contacts": "count(*)", "n_subscribed": "sum(sub)", "subscription_rate": "round(avg(CAST(sub AS double)), 5)"},
           {"job": "job", "marital": "marital", "education": "education", "contact": "contact", "month": "month",
            "dataset": "dataset",
            "age_band": "CASE WHEN age < 25 THEN '<25' WHEN age < 35 THEN '25-34' WHEN age < 45 THEN '35-44' "
                        "WHEN age < 55 THEN '45-54' WHEN age < 65 THEN '55-64' ELSE '65+' END"},
           {"job": "job", "dataset": "dataset", "exclude_unknown_job": "job <> 'unknown'"},
           order="subscription_rate DESC"),
    Metric("mbd_product_buyers",
           "MBD-mini (khách doanh nghiệp ẩn danh của Sber, 4 sản phẩm ẩn danh sp1..sp4, 12 tháng): số doanh nghiệp theo "
           "số sản phẩm KHÁC NHAU đã mua trong kỳ (0..4), số người mua từng sản phẩm, kèm tối đa 5 mã khách ví dụ.",
           ["silver_mbd.target_month"], MBD,
           {"n_clients": "count(*)", "buyers_sp1": "sum(b1)", "buyers_sp2": "sum(b2)", "buyers_sp3": "sum(b3)",
            "buyers_sp4": "sum(b4)", "example_client_ids": "slice(array_agg(client_id ORDER BY client_id), 1, 5)"},
           {"n_products_bought": "b1 + b2 + b3 + b4"}, {}, order="n_products_bought"),
    Metric("berka_loans",
           "Khoản vay Berka: số khoản, tổng và trung bình số tiền (CZK), tỉ lệ nợ có vấn đề (trạng thái B, D). Theo "
           "vùng, quận, trạng thái, năm giải ngân, kỳ hạn (tháng). Vùng/quận là của chi nhánh mở tài khoản.",
           ["silver_berka.loan", "silver_berka.account", "silver_berka.district"], BERKA_LOAN,
           {"n_loans": "count(*)", "total_amount_czk": "round(sum(amount), 0)", "avg_amount_czk": "round(avg(amount), 0)",
            "problem_rate": "round(avg(CASE WHEN is_problem_loan THEN 1.0 ELSE 0.0 END), 5)"},
           {"region": "region", "district": "district_name", "loan_status": "loan_status",
            "granted_year": "year(granted_date)", "duration_months": "duration_months"},
           {"region": "region", "loan_status": "loan_status"}, order="n_loans DESC"),
    Metric("berka_clients",
           "Số khách Berka theo vùng, quận (nơi ở), giới tính.",
           ["silver_berka.client", "silver_berka.district"], BERKA_CLIENT,
           {"n_clients": "count(DISTINCT client_id)"},
           {"region": "region", "district": "district_name", "gender": "gender"}, {"region": "region"},
           order="n_clients DESC"),
    Metric("aml_attempts",
           "Vụ rửa tiền IBM AML (HI-Medium): số vụ và số giao dịch thuộc vụ, theo kiểu hình (FAN-OUT, CYCLE, ...).",
           ["silver_aml.laundering_attempt"], AML_ATTEMPT,
           {"n_attempts": "count(DISTINCT attempt_id)", "n_transactions": "count(DISTINCT txn_id)"},
           {"typology": "typology"}, {"typology": "typology"}, order="n_attempts DESC"),
    Metric("paysim_fraud",
           "Ví điện tử PaySim: số giao dịch, số gian lận, tỉ lệ gian lận, số gian lận bị hệ thống cũ gắn cờ, theo loại "
           "giao dịch.",
           ["silver_paysim.transaction", "silver_paysim.fraud_label"], PAYSIM,
           {"n_txn": "count(*)", "n_fraud": "sum(fraud)", "fraud_rate": "round(avg(CAST(fraud AS double)), 6)",
            "n_flagged": "sum(flagged)", "total_amount": "round(sum(amount), 0)"},
           {"txn_type": "txn_type"}, {"txn_type": "txn_type"}, order="n_txn DESC"),
    Metric("card_category_spend",
           "Giao dịch thẻ theo nhóm ngành merchant (grocery_pos, shopping_net, travel, ...): số giao dịch, số tiền trung "
           "bình, trung vị, phân vị 90/99 (baseline chi tiêu, track_a_gold.card_category_baseline). Trả từng dòng.",
           ["track_a_gold.card_category_baseline"], "track_a_gold.card_category_baseline t",
           {"n_txn": "n_txn", "avg_amt": "round(avg_amt, 2)", "p50_amt": "round(p50_amt, 2)",
            "p90_amt": "round(p90_amt, 2)", "p99_amt": "round(p99_amt, 2)", "as_of": "as_of"},
           {"category": "category"}, {"category": "category"}, order="avg_amt DESC", aggregate=False),
    Metric("card_fraud_rules",
           "Hiệu quả rule gian lận thẻ realtime (backtest trên nhãn chargeback): precision, recall, F1, tỉ lệ cảnh báo "
           "theo rule (AMOUNT_SPIKE, NIGHT_HIGH_RISK, VELOCITY_2H, ANY_RULE, MULTI_RULE_2, MULTI_RULE_3), mức "
           "(txn = từng giao dịch, card_day = thẻ x ngày) và độ nghiêm trọng (any = mọi cảnh báo, high = chỉ mức cao). "
           "Bảng đã ở mức chi tiết nhất: trả từng dòng, không gộp; mặc định severity = any.",
           ["track_a_gold.card_rule_eval_summary"], "track_a_gold.card_rule_eval_summary t",
           {"n": "n", "n_fraud": "n_fraud", "tp": "tp", "fp": "fp", "fn": "fn",
            "precision": "round(precision, 4)", "recall": "round(recall, 4)", "f1": "round(f1, 4)",
            "alert_rate": "round(alert_rate, 6)", "eval_from": "eval_from", "eval_to": "eval_to"},
           {"rule": "rule", "level": "level", "severity": "severity"},
           {"rule": "rule", "level": "level", "severity": "severity"}, order="rule, level, severity",
           aggregate=False, default_filters={"severity": "any"}),
]}

_VALUE = re.compile(r"^[A-Za-z0-9 _.\-/&]{1,60}$")


def spec_text() -> str:
    """Danh sách chỉ số cho mô tả tool (ổn định từng byte)."""
    return " | ".join(f"{m.name}: by {sorted(m.dims)}, filters {sorted(m.filters)}" for m in METRICS.values())


def build_sql(metric: str, by: Optional[List[str]], filters: Optional[Dict[str, Any]]) -> str:
    m = METRICS.get(metric)
    if m is None:
        raise MetricError(f"khong co chi so {metric!r}; co: {sorted(METRICS)}")
    by = list(by or [])
    if len(by) > 2:
        raise MetricError("toi da 2 chieu trong `by`")
    bad = [d for d in by if d not in m.dims]
    if bad:
        raise MetricError(f"chieu {bad} khong hop le cho {metric}; duoc phep: {sorted(m.dims)}")
    where = []
    for k, v in {**m.default_filters, **(filters or {})}.items():
        if k not in m.filters:
            raise MetricError(f"bo loc {k!r} khong hop le cho {metric}; duoc phep: {sorted(m.filters)}")
        expr = m.filters[k]
        if " " in expr and v in (True, "true", 1):        # bộ lọc dạng cờ (vd exclude_unknown_job)
            where.append(expr)
            continue
        if not isinstance(v, (str, int)) or not _VALUE.match(str(v)):
            raise MetricError(f"gia tri bo loc {k}={v!r} khong hop le")
        where.append(f"{expr} = '{str(v)}'" if isinstance(v, str) else f"{expr} = {int(v)}")
    if not m.aggregate:
        by = list(m.dims)
    sel = [f"{m.dims[d]} AS {d}" for d in by] + [f"{e} AS {n}" for n, e in m.measures.items()]
    sql = f"SELECT {', '.join(sel)} FROM {m.base}"
    if where:
        sql += " WHERE " + " AND ".join(where)
    if by and m.aggregate:
        sql += " GROUP BY " + ", ".join(str(i + 1) for i in range(len(by)))
    cols = set(m.measures) | set(by)
    order = m.order if (m.order and all(o.split()[0] in cols for o in m.order.split(","))) else None
    if order:
        sql += f" ORDER BY {order}"
    return sql + " LIMIT 200"


def cite_id(metric: str, by: Optional[List[str]], filters: Optional[Dict[str, Any]]) -> str:
    key = json.dumps([metric, list(by or []), dict(sorted((filters or {}).items()))], ensure_ascii=False, default=str)
    return hashlib.md5(key.encode()).hexdigest()


def run(backend, metric: str, by: Optional[List[str]] = None, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    sql = build_sql(metric, by, filters)
    rows = backend.run_sql(sql)
    m = METRICS[metric]
    return {"metric": metric, "definition": m.description, "source_tables": m.source_tables,
            "cite_id": cite_id(metric, by, filters), "rows": rows,
            "note": "Trich [metric:<cite_id>] sau moi so lieu va neu ten bang nguon (source_tables)."}
