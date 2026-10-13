"""Sinh hồ sơ vay tổng hợp theo schema và phân phối gần đúng của Home Credit.

Dùng khi chưa tải bộ Kaggle (cần đồng ý luật cuộc thi), và cho simulator sinh hồ sơ
mới hằng ngày. Tỷ lệ theo thống kê công khai của application_train.csv:
hợp đồng tiền mặt ~90%, nữ ~66%, DAYS_EMPLOYED = 365243 (hưu/không đi làm) ~18%,
EXT_SOURCE_1 thiếu ~56%, tỷ lệ vỡ nợ ~8%.
"""

from __future__ import annotations

import math
import random
from typing import Dict, List, Optional, Tuple

INCOME = [("Working", .52), ("Commercial associate", .23), ("Pensioner", .18), ("State servant", .07)]
EDU = [("Secondary / secondary special", .71), ("Higher education", .24),
       ("Incomplete higher", .03), ("Lower secondary", .02)]
FAMILY = [("Married", .64), ("Single / not married", .15), ("Civil marriage", .10),
          ("Separated", .06), ("Widow", .05)]
HOUSING = [("House / apartment", .89), ("With parents", .05), ("Municipal apartment", .04),
           ("Rented apartment", .02)]
OCCUPATION = [(None, .31), ("Laborers", .18), ("Sales staff", .10), ("Core staff", .09),
              ("Managers", .07), ("Drivers", .06), ("High skill tech staff", .04),
              ("Accountants", .03), ("Medicine staff", .03), ("Security staff", .02),
              ("Cooking staff", .02), ("Cleaning staff", .02), ("Private service staff", .01),
              ("Low-skill Laborers", .01), ("Waiters/barmen staff", .01)]
ORG = [("Business Entity Type 3", .25), ("Self-employed", .14), ("Other", .06), ("Medicine", .04),
       ("Government", .04), ("Business Entity Type 2", .04), ("School", .03), ("Trade: type 7", .03),
       ("Kindergarten", .02), ("Construction", .02), ("Transport: type 4", .02), ("Industry: type 3", .01)]
CREDIT_TYPE = [("Consumer credit", .73), ("Credit card", .23), ("Car loan", .02), ("Mortgage", .02)]
PREV_STATUS = [("Approved", .62), ("Canceled", .19), ("Refused", .17), ("Unused offer", .02)]


def pick(rng: random.Random, table):
    x = rng.random() * sum(p for _, p in table)
    for v, p in table:
        x -= p
        if x <= 0:
            return v
    return table[-1][0]


def lognorm(rng: random.Random, median: float, sigma: float) -> float:
    return round(rng.lognormvariate(math.log(median), sigma), 2)


def default_prob(app: Dict) -> float:
    """Xác suất vỡ nợ: điểm tín dụng ngoài càng thấp càng dễ vỡ (~8% trung bình)."""
    e2 = app["ext_source_2"] or 0.5
    e3 = app["ext_source_3"] if app["ext_source_3"] is not None else 0.5
    z = -2.1 - 4.0 * (e2 - 0.5) - 3.0 * (e3 - 0.5) + (0.3 if app["code_gender"] == "M" else 0)
    return 1 / (1 + math.exp(-z))


def application(rng: random.Random, sk_id_curr: int) -> Dict:
    income_type = pick(rng, INCOME)
    pensioner = income_type == "Pensioner"
    credit = round(lognorm(rng, 513_531, 0.6), -3)
    months = rng.choice([12, 18, 24, 36, 48, 60])
    return {
        "sk_id_curr": sk_id_curr,
        "name_contract_type": "Revolving loans" if rng.random() < 0.10 else "Cash loans",
        "code_gender": "F" if rng.random() < 0.66 else "M",
        "flag_own_car": "Y" if rng.random() < 0.34 else "N",
        "flag_own_realty": "Y" if rng.random() < 0.69 else "N",
        "cnt_children": min(int(rng.expovariate(2.4)), 6),
        "amt_income_total": round(lognorm(rng, 147_150, 0.5), -2),
        "amt_credit": credit,
        "amt_annuity": round(credit * (1.12 ** (months / 12)) / months, 1),
        "amt_goods_price": round(credit * 0.9, -3),
        "name_income_type": income_type,
        "name_education_type": pick(rng, EDU),
        "name_family_status": pick(rng, FAMILY),
        "name_housing_type": pick(rng, HOUSING),
        "days_birth": -rng.randint(21 * 365, 69 * 365) if not pensioner else -rng.randint(55 * 365, 69 * 365),
        "days_employed": 365243 if pensioner else -rng.randint(30, 15_000),
        "occupation_type": None if pensioner else pick(rng, OCCUPATION),
        "organization_type": "XNA" if pensioner else pick(rng, ORG),
        "ext_source_1": round(rng.betavariate(5, 5), 6) if rng.random() < 0.44 else None,
        "ext_source_2": round(rng.betavariate(5, 3), 6),
        "ext_source_3": round(rng.betavariate(4, 3), 6) if rng.random() < 0.80 else None,
    }


def bureau(rng: random.Random, sk_id_curr: int, next_id: int) -> Tuple[List[Dict], List[Dict]]:
    rows, balances = [], []
    for i in range(min(int(rng.expovariate(1 / 4.5)), 15)):
        active = rng.random() < 0.37
        days_credit = -rng.randint(30, 2900)
        amt = round(lognorm(rng, 125_000, 1.1), -2)
        debt = round(amt * rng.uniform(0.1, 0.9), 2) if active else 0.0
        bid = next_id + i
        overdue = rng.choice([0] * 30 + [5, 30, 60]) if active else 0
        rows.append({
            "sk_id_bureau": bid, "sk_id_curr": sk_id_curr,
            "credit_active": "Active" if active else "Closed", "credit_currency": "currency 1",
            "days_credit": days_credit, "credit_day_overdue": overdue,
            "days_credit_enddate": days_credit + rng.randint(180, 1800),
            "amt_credit_sum": amt, "amt_credit_sum_debt": debt,
            "amt_credit_sum_overdue": round(debt * 0.1, 2) if overdue else 0.0,
            "credit_type": pick(rng, CREDIT_TYPE),
            "days_credit_update": -rng.randint(0, abs(days_credit)),
            "amt_annuity": round(amt / 24, 1) if rng.random() < 0.3 else None,
        })
        n_months = min(abs(days_credit) // 30, 24)
        for m in range(n_months):
            status = "C" if (not active and m < 3) else ("X" if rng.random() < 0.1 else
                     ("0" if rng.random() < 0.95 else str(rng.randint(1, 5))))
            balances.append({"sk_id_bureau": bid, "months_balance": -m, "status": status})
    return rows, balances


def previous(rng: random.Random, sk_id_curr: int, next_id: int):
    prevs, pos, cc, inst = [], [], [], []
    for i in range(min(int(rng.expovariate(1 / 2.2)), 8)):
        pid = next_id + i
        status = pick(rng, PREV_STATUS)
        revolving = rng.random() < 0.12
        amt = round(lognorm(rng, 112_000, 1.0), -2)
        cnt = 0 if revolving else rng.choice([6, 10, 12, 18, 24, 36])
        decision = -rng.randint(30, 2900)
        prevs.append({
            "sk_id_prev": pid, "sk_id_curr": sk_id_curr,
            "name_contract_type": "Revolving loans" if revolving else rng.choice(["Cash loans", "Consumer loans"]),
            "amt_annuity": round(amt / max(cnt, 12) * 1.1, 1), "amt_application": amt,
            "amt_credit": amt if status == "Approved" else 0.0, "amt_goods_price": amt,
            "name_contract_status": status, "days_decision": decision,
            "name_payment_type": "Cash through the bank" if rng.random() < 0.62 else "XNA",
            "code_reject_reason": "XAP" if status != "Refused" else rng.choice(["HC", "LIMIT", "SCO", "SCOFR"]),
            "name_client_type": rng.choice(["Repeater", "Repeater", "New", "Refreshed"]),
            "cnt_payment": cnt, "name_yield_group": rng.choice(["middle", "high", "low_normal", "XNA"]),
        })
        if status != "Approved":
            continue
        start_month = decision // 30
        if revolving:
            limit = round(lognorm(rng, 90_000, 0.6), -3)
            for m in range(min(abs(start_month), 24)):
                draw = round(limit * rng.uniform(0, 0.4), 2) if rng.random() < 0.4 else 0.0
                cc.append({"sk_id_prev": pid, "sk_id_curr": sk_id_curr, "months_balance": -m,
                           "amt_balance": round(limit * rng.uniform(0, 0.8), 2),
                           "amt_credit_limit_actual": limit, "amt_drawings_current": draw,
                           "amt_payment_current": round(draw * rng.uniform(0.5, 1.2), 2),
                           "amt_total_receivable": round(limit * rng.uniform(0, 0.8), 2),
                           "cnt_drawings_current": int(draw > 0) * rng.randint(1, 5),
                           "name_contract_status": "Active", "sk_dpd": 0})
            continue
        annuity = prevs[-1]["amt_annuity"]
        for k in range(1, cnt + 1):
            month = start_month + k
            if month > 0:
                break
            late = rng.random() < 0.07
            due = decision + 30 * k
            pos.append({"sk_id_prev": pid, "sk_id_curr": sk_id_curr, "months_balance": month,
                        "cnt_instalment": cnt, "cnt_instalment_future": cnt - k,
                        "name_contract_status": "Completed" if k == cnt else "Active",
                        "sk_dpd": rng.randint(1, 30) if late else 0, "sk_dpd_def": 0})
            inst.append({"sk_id_prev": pid, "sk_id_curr": sk_id_curr, "num_instalment_version": 1.0,
                         "num_instalment_number": k, "days_instalment": float(due),
                         "days_entry_payment": float(due + (rng.randint(1, 40) if late else -rng.randint(0, 10))),
                         "amt_instalment": annuity,
                         "amt_payment": annuity if rng.random() > 0.03 else round(annuity * 0.5, 3)})
    return prevs, pos, cc, inst


def maybe(rng: random.Random, p: float, value) -> Optional[object]:
    return value if rng.random() < p else None
