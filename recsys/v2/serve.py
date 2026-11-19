"""Tầng phục vụ của hệ gợi ý v2: từ xác suất mô hình -> danh sách gợi ý người dùng cuối đọc được.

Bốn bước sau mô hình (docs/RECSYS-V2.md mục 7):
  1. Đủ điều kiện: bỏ sản phẩm khách đang giữ (đã có trong dữ liệu) + luật nghiệp vụ (config.BERKA_RULES).
  2. Chính sách liên hệ: không chào lại sản phẩm khách vừa bỏ qua (cool-down) hoặc vừa nhận; đọc từ nhật ký sự kiện.
  3. Xếp hạng theo GIÁ TRỊ KỲ VỌNG = p_cal x giá trị sản phẩm; bỏ gợi ý có p_cal < min_p_show ("không gợi ý còn hơn
     gợi ý tồi").
  4. Giải thích: 3 đặc trưng SHAP đóng góp lớn nhất -> câu tiếng Việt; kèm gợi ý cách nói chuyện cho RM.

Xuất: DataFrame `reco_topk` (một dòng mỗi (khách, hạng)) -> parquet + bảng Iceberg track_a_gold.reco_topk (publish).
"""

from __future__ import annotations

import datetime as dt
import pathlib
from typing import Dict, Iterable, List, Optional

import numpy as np
import pandas as pd

from .config import BERKA_RULES, Domain
from .data import Dataset
from .model import Ranker

REASON_VI = {
    # MBD
    "own_lag1": "đã mua sản phẩm này tháng trước", "own_cnt": "số lần đã mua sản phẩm này", "own_since":
    "số tháng từ lần mua cuối", "own_rate": "tần suất mua sản phẩm này", "own_ever": "đã từng mua sản phẩm này",
    "own_recent3": "mua sản phẩm này trong 3 tháng gần đây", "trx_m": "số giao dịch tháng này", "trx_3m":
    "số giao dịch 3 tháng", "trx_mean6": "giao dịch trung bình 6 tháng", "trx_trend": "giao dịch tăng so với trước",
    "any_cnt": "tổng số lần mua mọi sản phẩm", "any_recent3": "vừa mua sản phẩm khác gần đây", "midx": "thâm niên quan sát",
    "zero_streak": "số tháng liên tiếp không giao dịch", "log_tc": "tổng giao dịch luỹ kế", "dtd": "khoảng ngày giao dịch",
    "n_products_ever": "số loại sản phẩm đã từng mua",
    # Berka
    "avg_balance_3m": "số dư bình quân 3 tháng", "inflow_3m": "tiền vào 3 tháng", "outflow_3m": "tiền ra 3 tháng",
    "txn_cnt_3m": "số giao dịch 3 tháng", "cash_wd_cnt_3m": "rút tiền mặt thường xuyên", "card_wd_cnt_3m":
    "rút tiền bằng thẻ", "tenure_months": "thời gian gắn bó với ngân hàng", "pension_3m": "nhận lương hưu",
    "net_flow_3m": "dòng tiền ròng 3 tháng", "inflow_trend": "tiền vào tăng", "age": "độ tuổi", "n_products":
    "số sản phẩm đang dùng", "end_balance": "số dư cuối tháng", "min_balance_3m": "số dư thấp nhất 3 tháng",
    "district_avg_salary": "lương trung bình khu vực", "has_household": "đang dùng thanh toán định kỳ",
    "has_insurance": "đang có bảo hiểm", "has_loan": "đang có khoản vay", "has_card_classic": "đang có thẻ classic",
    "product": "đặc điểm chung của sản phẩm",
}


def reason_text(name: str) -> str:
    return REASON_VI.get(name, name.replace("_", " "))


def rule_mask(ds: Dataset, d: pd.DataFrame) -> np.ndarray:
    """(n, K) True = qua luật nghiệp vụ. Miền không có luật thì toàn True."""
    M = np.ones((len(d), len(ds.domain.codes)), bool)
    if ds.domain.name != "berka":
        return M
    recs = d.to_dict("records")
    for code, _, fn in BERKA_RULES:
        j = ds.domain.codes.index(code)
        M[:, j] = [bool(fn(r)) for r in recs]
    return M


def suppression(events: Iterable[Dict], domain: Domain, today: Optional[dt.date] = None) -> Dict[tuple, str]:
    """(client_id, product) -> lý do đang bị chặn chào (cool-down sau dismiss / accept)."""
    today = today or dt.date.today()
    out = {}
    for e in events:
        days = domain.cooldown_days.get(e.get("event_type"), 0)
        if not days:
            continue
        when = dt.date.fromisoformat(str(e["ts"])[:10])
        if (today - when).days < days:
            out[(str(e["client_id"]), str(e["product"]))] = f"{e['event_type']} ngày {when}, chờ {days} ngày"
    return out


def recommend(ds: Dataset, d: pd.DataFrame, model: Ranker, k: int = 3, events: Iterable[Dict] = (),
              variant: str = "treatment", today: Optional[dt.date] = None, explain: bool = True) -> pd.DataFrame:
    dom = ds.domain
    P = model.predict(ds, d)
    elig = d[[f"elig_{c}" for c in dom.codes]].astype(bool).to_numpy()
    rules = rule_mask(ds, d)
    sup = suppression(events, dom, today)
    ids = d[ds.id_col].astype(str).to_numpy()
    blocked = np.array([[(ids[i], c) in sup for c in dom.codes] for i in range(len(d))]) if sup else np.zeros_like(elig)
    value = np.array([p.value for p in dom.products])
    ok = elig & rules & ~blocked & (P >= dom.min_p_show)
    EV = np.where(ok, P * value[None, :], -np.inf)
    order = np.argsort(-EV, axis=1, kind="stable")[:, :k]
    rows = []
    reasons = {}
    if explain:
        for r in range(min(k, len(dom.codes))):
            reasons[r] = model.contributions(ds, d, order[:, r])
    for i in range(len(d)):
        for r in range(order.shape[1]):
            j = order[i, r]
            if not np.isfinite(EV[i, j]):
                continue
            prod = dom.products[j]
            why = "; ".join(reason_text(n) for n, _ in reasons.get(r, [[]] * len(d))[i]) if explain else ""
            rows.append({"domain": dom.name, "client_id": ids[i], "scored_month": str(d[ds.month_col].iat[i]),
                         "rank": r + 1, "product": prod.code, "product_label": prod.label_vi, "product_iri": prod.iri,
                         "p_calibrated": float(P[i, j]), "expected_value": float(EV[i, j]), "reasons": why,
                         "pitch": prod.pitch, "variant": variant, "model_version": model.meta.get("version", ""),
                         "created_at": dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")})
    out = pd.DataFrame(rows)
    out.attrs["filtered"] = {"not_eligible": int((~elig).sum()), "rule": int((elig & ~rules).sum()),
                             "suppressed": int((elig & rules & blocked).sum()),
                             "below_min_p": int((elig & rules & ~blocked & (P < dom.min_p_show)).sum())}
    return out


def latest(ds: Dataset) -> pd.DataFrame:
    """Tháng chụp mới nhất của mỗi khách (chấm điểm để phục vụ)."""
    d = ds.df.sort_values(ds.month_col)
    return d.groupby(ds.id_col, sort=False).tail(1).reset_index(drop=True)


# ---------------------------------------------------------------------------------------------- xuất ra lake
def publish(df: pd.DataFrame, table: str = "reco_topk", profile: str = "fg-analyst-mcp",
            workgroup: str = "fingraph-dev-analyst", database: str = "track_a_gold", region: str = "ap-southeast-1",
            log=print) -> str:
    """Ghi bảng gợi ý vào track_a_gold dưới persona ANALYST (quyền ghi gold đã có: IAM S3GhiGold + LF CREATE_TABLE).

    Cách làm: parquet -> s3://<gold>/_staging/<table>/<run>/ -> bảng external tạm -> CTAS Iceberg <table> (thay bản cũ)
    -> xoá bảng tạm. Bảng do analyst tạo nên analyst có ALL trên nó (drop/thay được), không đụng bảng của pipeline."""
    import io

    import boto3

    sess = boto3.Session(profile_name=profile, region_name=region)
    glue, s3 = sess.client("glue"), sess.client("s3")
    loc = glue.get_database(Name=database)["Database"]["LocationUri"].rstrip("/")
    bucket, _, prefix = loc.removeprefix("s3://").partition("/")
    run = dt.datetime.now().strftime("%Y%m%d%H%M%S")
    key = f"{prefix}/_staging/{table}/{run}/part-0.parquet"
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    log(f"  ghi s3://{bucket}/{key} ({len(df):,} dong)")
    from agent.tools import AthenaBackend

    be = AthenaBackend(workgroup, session=sess, region=region, timeout_s=300)
    stage = f"{table}_stage_{run}"
    types = {"int64": "bigint", "float64": "double", "bool": "boolean"}
    cols = ", ".join(f"`{c}` {types.get(str(t), 'string')}" for c, t in df.dtypes.items())
    be.run_sql(f"CREATE EXTERNAL TABLE {database}.{stage} ({cols}) STORED AS PARQUET "
               f"LOCATION 's3://{bucket}/{prefix}/_staging/{table}/{run}/'")
    be.run_sql(f"DROP TABLE IF EXISTS {database}.{table}")
    be.run_sql(f"CREATE TABLE {database}.{table} WITH (table_type = 'ICEBERG', is_external = false, "
               f"location = 's3://{bucket}/{prefix}/{table}/{run}/', format = 'PARQUET') AS SELECT * FROM {database}.{stage}")
    be.run_sql(f"DROP TABLE IF EXISTS {database}.{stage}")
    n = be.run_sql(f"SELECT count(*) AS n FROM {database}.{table}")[0]["n"]
    log(f"  {database}.{table}: {n} dong (Iceberg)")
    return f"{database}.{table}"


def save_local(df: pd.DataFrame, out: pathlib.Path) -> pathlib.Path:
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    return out
