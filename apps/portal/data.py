"""Truy vấn và logic của FinGraph Portal, tách khỏi giao diện để test không cần AWS.

Mọi truy vấn chạy bằng credential của PERSONA người dùng (profile AWS của role persona), qua workgroup
Athena của persona đó. Lake Formation lọc ở tầng lưu trữ: analyst không thấy cột PII, khách hàng chỉ
thấy dòng của mình. Portal KHÔNG tự lọc hay che gì thêm, nên phân quyền chỉ có một nơi quyết định.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

REGION = os.environ.get("AWS_REGION", "ap-southeast-1")
ENV = os.environ.get("FINGRAPH_ENV", "dev")
PREFIX = f"fingraph-{ENV}"


@dataclass(frozen=True)
class Persona:
    key: str
    label: str
    profile: str          # profile AWS (credential của role persona)
    workgroup: str        # workgroup Athena của persona
    graph: bool = False   # được gọi Lambda neptune-ops
    ops: bool = False     # được xem Glue job / vận hành
    customer: bool = False


def personas() -> Dict[str, Persona]:
    """Profile đổi được bằng biến môi trường (scripts/assume-all.ps1 của repo hạ tầng sinh *-mcp)."""
    e = os.environ.get
    return {
        "analyst": Persona("analyst", "Chuyên viên phân tích / RM / AML (DA-BA)",
                           e("FINGRAPH_PROFILE_ANALYST", "fg-analyst-mcp"), f"{PREFIX}-analyst"),
        "de": Persona("de", "Data engineer", e("FINGRAPH_PROFILE_DE", "fg-de-mcp"),
                      f"{PREFIX}-data-engineer", graph=True, ops=True),
        "customer": Persona("customer", "Khách hàng (client 2)", e("FINGRAPH_PROFILE_CUSTOMER", "fg-client2-mcp"),
                            f"{PREFIX}-customer-client-2", customer=True),
    }


# ------------------------------------------------------------------ chạy Athena

class QueryError(RuntimeError):
    pass


def run_sql(session, workgroup: str, sql: str, timeout_s: int = 90, max_rows: int = 1000) -> List[Dict[str, Any]]:
    athena = session.client("athena", region_name=REGION)
    qid = athena.start_query_execution(QueryString=sql, WorkGroup=workgroup)["QueryExecutionId"]
    deadline = time.monotonic() + timeout_s
    while True:
        st = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if st["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if time.monotonic() > deadline:
            athena.stop_query_execution(QueryExecutionId=qid)
            raise QueryError(f"Truy vấn quá {timeout_s}s")
        time.sleep(0.6)
    if st["State"] != "SUCCEEDED":
        raise QueryError(explain_error(st.get("StateChangeReason", "")))
    out: List[Dict[str, Any]] = []
    head: List[str] = []
    for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        for i, r in enumerate(page["ResultSet"]["Rows"]):
            vals = [c.get("VarCharValue") for c in r["Data"]]
            if not head:
                head = vals
                continue
            out.append(dict(zip(head, vals)))
            if len(out) >= max_rows:
                return out
    return out


def explain_error(reason: str) -> str:
    """Dịch lỗi quyền của Lake Formation sang câu người dùng hiểu."""
    r = reason or ""
    if "COLUMN_NOT_FOUND" in r and "not authorized" in r:
        col = re.search(r"Column '([^']+)'", r)
        return (f"Bạn không có quyền xem cột {col.group(1) if col else ''} (dữ liệu cá nhân được che theo "
                "vai trò). Bỏ cột đó khỏi câu truy vấn.")
    if "Insufficient permissions" in r or "AccessDenied" in r or "not authorized" in r:
        return "Vai trò hiện tại không được đọc bảng này (Lake Formation từ chối)."
    return r[:400]


# ------------------------------------------------------------------ SQL tự phục vụ

_FORBIDDEN = re.compile(r"\b(insert|update|delete|merge|drop|create|alter|truncate|grant|revoke|msck|unload|"
                        r"optimize|vacuum|call|prepare|execute|set|reset|use)\b", re.IGNORECASE)
_START = re.compile(r"^(select|with)\b", re.IGNORECASE)
_LIMIT = re.compile(r"\blimit\s+\d+\s*$", re.IGNORECASE)


def readonly_sql(sql: str, max_rows: int = 1000) -> str:
    """Một câu SELECT/WITH, không từ khoá ghi/DDL, tự thêm LIMIT. KHÔNG giới hạn database: đọc được bảng
    nào là việc của Lake Formation (khác agent, vốn chỉ được đọc tầng onto/gold)."""
    s = (sql or "").strip().rstrip(";").strip()
    if not s:
        raise ValueError("Câu truy vấn trống")
    if ";" in s:
        raise ValueError("Chỉ một câu lệnh")
    if not _START.match(s):
        raise ValueError("Chỉ cho SELECT / WITH")
    body = re.sub(r"'(?:[^']|'')*'", "''", s)          # bỏ chuỗi literal trước khi tìm từ khoá
    if _FORBIDDEN.search(body):
        raise ValueError("Câu lệnh có từ khoá ghi / DDL")
    if not _LIMIT.search(s):
        s += f" LIMIT {max_rows}"
    return s


# ------------------------------------------------------------------ SQL dùng chung

_ID = re.compile(r"^\d{1,9}$")


def _int(v: Any, name: str) -> int:
    s = str(v).strip()
    if not _ID.match(s):
        raise ValueError(f"{name} phải là số nguyên dương")
    return int(s)


def sql_client_latest(client_id: Any) -> str:
    c = _int(client_id, "client_id")
    return f"""
SELECT client_id, month, gender, age, region, district_id, tenure_months, n_accounts, n_products,
       has_loan, has_card_classic, has_card_junior, has_card_gold, has_insurance, has_household,
       end_balance, avg_balance_3m, inflow_3m, outflow_3m, txn_cnt_3m
FROM track_a_gold.berka_client_month
WHERE client_id = {c}
ORDER BY month DESC LIMIT 1""".strip()


def sql_client_history(client_id: Any) -> str:
    c = _int(client_id, "client_id")
    return (f"SELECT month, end_balance, inflow, outflow, txn_cnt, n_products FROM track_a_gold.berka_client_month "
            f"WHERE client_id = {c} ORDER BY month")


def sql_customer_uid(client_id: Any) -> str:
    c = _int(client_id, "client_id")
    return (f"SELECT uid, class_iri, label FROM track_a_onto.entity WHERE source_system = 'berka' "
            f"AND natural_key = 'client_id={c}' AND class_iri LIKE '%Customer%' LIMIT 1")


# Sản phẩm UC2 (track_a_pretrain.uc2_next_product.product) -> cờ đang sở hữu ở gold
PRODUCTS = {
    "loan": ("has_loan", "Khoản vay"),
    "card_classic": ("has_card_classic", "Thẻ Classic"),
    "card_junior": ("has_card_junior", "Thẻ Junior"),
    "card_gold": ("has_card_gold", "Thẻ Gold"),
    "insurance": ("has_insurance", "Bảo hiểm"),
    "household": ("has_household", "Thanh toán hộ gia đình"),
}


def sql_segment_rates(region: str, age: Any) -> str:
    """Tỉ lệ mua sản phẩm trong HORIZON tháng tới của nhóm khách cùng vùng và cùng nhóm tuổi 10 năm,
    so với toàn bộ (lift). Chỉ dùng tập train của UC2, để không lộ nhãn tập test."""
    reg = str(region).replace("'", "''")[:60]
    a = _int(int(float(age)), "age")
    band = a // 10
    return f"""
WITH base AS (
  SELECT product, avg(CAST(label AS double)) AS rate_all FROM track_a_pretrain.uc2_next_product
  WHERE split = 'train' GROUP BY product),
seg AS (
  SELECT product, avg(CAST(label AS double)) AS rate_seg, count(*) AS n FROM track_a_pretrain.uc2_next_product
  WHERE split = 'train' AND region = '{reg}' AND CAST(floor(age / 10) AS integer) = {band}
  GROUP BY product)
SELECT s.product, s.rate_seg, b.rate_all, s.n FROM seg s JOIN base b ON b.product = s.product""".strip()


# Nhãn UC2 của insurance/household lệch do cách mở tài khoản Berka (artefact, docs/RECSYS.md mục 3.2):
# không gợi ý hai sản phẩm này cho tới khi nhãn được sửa.
RECOMMENDABLE = ("loan", "card_classic", "card_junior", "card_gold")


def recommend(profile: Dict[str, Any], rates: Sequence[Dict[str, Any]], min_n: int = 30,
              top: int = 3, allowed: Sequence[str] = RECOMMENDABLE) -> List[Dict[str, Any]]:
    """Gợi ý sản phẩm baseline: sản phẩm khách CHƯA có, xếp theo tỉ lệ mua của nhóm khách tương tự.

    Nhóm quá nhỏ (< min_n dòng) thì dùng tỉ lệ toàn bộ để không gợi ý theo nhiễu. lift = nhóm / toàn bộ.
    Đây là mốc so sánh cho model gợi ý (pipeline pretrain), không phải model.
    """
    out = []
    for r in rates:
        p = r.get("product")
        if p not in PRODUCTS or p not in allowed:
            continue
        flag, name = PRODUCTS[p]
        if str(profile.get(flag, "")).lower() in ("true", "1"):
            continue
        n = int(float(r.get("n") or 0))
        all_ = float(r.get("rate_all") or 0.0)
        seg = float(r.get("rate_seg") or 0.0) if n >= min_n else all_
        out.append({"product": p, "name": name, "score": seg, "rate_all": all_,
                    "lift": (seg / all_) if all_ > 0 else None, "n_segment": n,
                    "basis": "nhóm tương tự" if n >= min_n else "toàn bộ khách (nhóm quá nhỏ)"})
    out.sort(key=lambda x: (-x["score"], x["product"]))
    return out[:top]


# ------------------------------------------------------------------ gian lận thẻ

SQL_FRAUD_SUMMARY = ("SELECT rule, severity, level, n, n_fraud, tp, fp, fn, precision, recall, f1, alert_rate, "
                     "eval_from, eval_to, universe FROM track_a_gold.card_rule_eval_summary ORDER BY level, rule")
# level: 'txn' (mỗi giao dịch) hoặc 'card_day' (gộp theo thẻ-ngày); severity: 'any' | 'high'
SQL_FRAUD_DAILY = ("SELECT txn_date, rule, precision, recall, f1, alert_rate FROM track_a_gold.card_rule_eval_daily "
                   "WHERE level = 'txn' ORDER BY txn_date")


# ------------------------------------------------------------------ khách hàng tự xem (LF lọc dòng)

SQL_MY_ACCOUNTS = ("SELECT account_id, district_id, opened_date, statement_frequency FROM silver_berka.account "
                   "ORDER BY account_id")
SQL_MY_TRANS = ("SELECT trans_date, account_id, direction, operation_type, amount, balance_after, purpose "
                "FROM silver_berka.trans ORDER BY trans_date DESC LIMIT 50")
SQL_MY_LOANS = ("SELECT loan_id, account_id, granted_date, amount, duration_months, monthly_payment, loan_status "
                "FROM silver_berka.loan ORDER BY granted_date")
SQL_MY_BALANCE = ("SELECT date_trunc('month', trans_date) AS month, sum(signed_amount) AS net, "
                  "max_by(balance_after, trans_date) AS end_balance FROM silver_berka.trans GROUP BY 1 ORDER BY 1")


# ------------------------------------------------------------------ vận hành (DE)

def glue_job_runs(session, prefix: str = PREFIX, per_job: int = 3) -> List[Dict[str, Any]]:
    glue = session.client("glue", region_name=REGION)
    names = [j for page in glue.get_paginator("list_jobs").paginate() for j in page["JobNames"]
             if j.startswith(prefix)]
    rows = []
    for n in sorted(names):
        runs = glue.get_job_runs(JobName=n, MaxResults=per_job).get("JobRuns", [])
        for r in runs:
            args = r.get("Arguments", {})
            rows.append({"job": n, "entry": args.get("--JOB_NAME") or args.get("--TRANSFORM", ""),
                         "state": r["JobRunState"], "started": str(r.get("StartedOn", ""))[:19],
                         "seconds": r.get("ExecutionTime"), "error": (r.get("ErrorMessage") or "")[:120]})
    rows.sort(key=lambda x: x["started"], reverse=True)
    return rows


def visible_tables(session, databases: Optional[Sequence[str]] = None) -> List[Dict[str, Any]]:
    """Bảng persona THẤY được: Glue trả metadata đã lọc theo grant DESCRIBE của Lake Formation."""
    glue = session.client("glue", region_name=REGION)
    dbs = databases or [d["Name"] for p in glue.get_paginator("get_databases").paginate() for d in p["DatabaseList"]]
    rows = []
    for db in dbs:
        try:
            for p in glue.get_paginator("get_tables").paginate(DatabaseName=db):
                for t in p["TableList"]:
                    rows.append({"database": db, "table": t["Name"],
                                 "columns": len(t.get("StorageDescriptor", {}).get("Columns", [])),
                                 "updated": str(t.get("UpdateTime", ""))[:19]})
        except Exception:  # database không có quyền DESCRIBE
            continue
    return rows


# ------------------------------------------------------------------ gợi ý bằng model (recsys, MBD)

def recsys_available() -> Optional[str]:
    """None nếu dùng được recsys (code + model LightGBM đã train); ngược lại là lý do."""
    try:
        from recsys import mbd, recommend as R  # noqa: F401
    except ImportError as e:
        return f"Chưa có package recsys ({e}). Cần nhánh recsys được merge vào main."
    missing = [p for p in mbd.PRODUCTS if not (R.REPORTS / f"mbd_{p}.lgb.txt").exists()]
    if missing:
        return f"Chưa có model {missing} trong {R.REPORTS}: chạy `python -m recsys.mbd` để huấn luyện."
    return None


def sql_mbd_features(client_id: str) -> str:
    """Đặc trưng recsys.mbd của MỘT khách, tháng báo cáo mới nhất. Bọc nguyên FEATURE_SQL (cửa sổ tính trên
    toàn lịch sử của khách) rồi mới lọc, để đặc trưng giống hệt lúc huấn luyện."""
    from recsys import mbd

    cid = str(client_id).strip()
    if not re.fullmatch(r"[A-Za-z0-9_\-]{1,64}", cid):
        raise ValueError("client_id MBD chỉ gồm chữ, số, '_' hoặc '-'")
    lit = cid if cid.isdigit() else f"'{cid}'"
    return (f"SELECT * FROM ({mbd.FEATURE_SQL.strip().rstrip(';')}) x "
            f"WHERE CAST(client_id AS varchar) = CAST({lit} AS varchar) ORDER BY report_month DESC LIMIT 1")


def recommend_mbd(session, workgroup: str, client_id: str, k: int = 3):
    """Top-k sản phẩm MBD bằng model LightGBM của recsys, chạy bằng credential của persona (không dùng
    profile mặc định fg-de-mcp của recsys.data.athena_df)."""
    import pandas as pd
    from recsys import mbd, recommend as R

    df = pd.DataFrame(run_sql(session, workgroup, sql_mbd_features(client_id), timeout_s=180))
    if df.empty:
        return df
    for c in mbd.FEATURES:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return R.score(df, R.load_models(), k)


# ------------------------------------------------------------------ hệ gợi ý v2 (recsys/v2, docs/RECSYS-V2.md)
# Bảng do `python -m recsys.v2.run publish --domain berka` ghi (persona analyst). Khách đọc reco_topk_berka qua filter
# dòng Lake Formation (repo hạ tầng, customer_gold_tables): portal KHÔNG tự lọc, không có đường vòng.
RECO_TABLE = "track_a_gold.reco_topk_berka"
RECO_SCORE_TABLE = "track_a_gold.reco_score_berka"
RECO_PRODUCTS = ("loan", "card_classic", "card_junior", "card_gold")
RECO_COLS = "rank, product, product_label, p_calibrated, expected_value, reasons, pitch, variant, model_version, scored_month"


def sql_reco_client(client_id: Any) -> str:
    return f"SELECT {RECO_COLS} FROM {RECO_TABLE} WHERE client_id = '{_int(client_id, 'client_id')}' ORDER BY rank"


SQL_MY_RECO = f"SELECT client_id, {RECO_COLS} FROM {RECO_TABLE} ORDER BY rank"      # LF chỉ trả dòng của khách đăng nhập


def sql_campaign(product: str, limit: Any) -> str:
    if product not in RECO_PRODUCTS:
        raise ValueError(f"san pham phai thuoc {RECO_PRODUCTS}")
    n = max(1, min(_int(limit, "limit"), 5000))
    return (f"SELECT client_id, p_calibrated, expected_value, scored_month, model_version FROM {RECO_SCORE_TABLE} "
            f"WHERE product = '{product}' ORDER BY p_calibrated DESC LIMIT {n}")


def sql_campaign_size(product: str) -> str:
    if product not in RECO_PRODUCTS:
        raise ValueError(f"san pham phai thuoc {RECO_PRODUCTS}")
    return (f"SELECT count(*) AS n, sum(p_calibrated) AS expected FROM {RECO_SCORE_TABLE} WHERE product = '{product}'")


def reco_event_log():
    """Nhật ký sự kiện gợi ý (impression / click / accept / dismiss) cho đánh giá online. File JSONL cục bộ;
    đổi bằng FINGRAPH_RECO_EVENTS. Không chứa PII: chỉ mã khách, sản phẩm, nhóm thử nghiệm."""
    from recsys.v2.online import EventLog

    root = Path(__file__).resolve().parents[2]
    return EventLog(Path(os.environ.get("FINGRAPH_RECO_EVENTS", root / "recsys" / "v2" / "events" / "berka.jsonl")))


def reco_model_card(domain: str = "berka") -> Optional[Dict[str, Any]]:
    import json

    reg = Path(__file__).resolve().parents[2] / "recsys" / "v2" / "registry" / domain
    champ = reg / "CHAMPION"
    if not champ.exists():
        return None
    card = reg / champ.read_text(encoding="utf-8").strip() / "model_card.json"
    return json.loads(card.read_text(encoding="utf-8")) if card.exists() else None
