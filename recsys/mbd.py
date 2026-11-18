"""Gợi ý sản phẩm trên MBD-mini (Sber AI Lab): 100.224 khách x 12 tháng, 4 sản phẩm (target_1..4 = khách mua
sản phẩm k trong THÁNG SAU ngày báo cáo report_month).

    python -m recsys.mbd            # chay EDA + train + danh gia, ghi recsys/reports/mbd.md

Chống rò rỉ: dòng (khách, m) chỉ dùng target của các dòng TRƯỚC m. target của dòng m-1 là việc mua trong tháng m,
đã biết vào ngày báo cáo m; target của chính dòng m là nhãn. trans_count trong silver là LUỸ KẾ (tăng 11 -> 188 theo
tháng), nên đặc trưng hoạt động là chênh lệch giữa các tháng.

Chia theo thời gian (không ngẫu nhiên): train report_month <= 2022-10, valid 2022-11, test 2022-12 và 2023-01.
"""

from __future__ import annotations

import json
import pathlib

import numpy as np

from .data import athena_df
from .trainer import Spec, fit, report

PRODUCTS = ["sp1", "sp2", "sp3", "sp4"]
LABELS = [f"target_{i}" for i in range(1, 5)]
VALID, TEST = "2022-11-30", "2022-12-31"

FEATURE_SQL = """
WITH b AS (
  SELECT client_id, report_month, fold,
         CAST(target_1 AS integer) t1, CAST(target_2 AS integer) t2, CAST(target_3 AS integer) t3,
         CAST(target_4 AS integer) t4, trans_count tc, diff_trans_date dtd,
         row_number() OVER (PARTITION BY client_id ORDER BY report_month) - 1 AS midx
  FROM silver_mbd.target_month
), f AS (
  SELECT b.*,
""" + ",\n".join(
    f"""    lag(t{k}, {l}) OVER w AS t{k}_lag{l}""" for k in range(1, 5) for l in (1, 2, 3)
) + ",\n" + ",\n".join(
    f"""    coalesce(sum(t{k}) OVER (PARTITION BY client_id ORDER BY report_month ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS t{k}_cnt,
    max(CASE WHEN t{k} = 1 THEN midx END) OVER (PARTITION BY client_id ORDER BY report_month ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS t{k}_last_idx"""
    for k in range(1, 5)
) + """,
    tc - coalesce(lag(tc, 1) OVER w, 0) AS trx_m,
    coalesce(lag(tc, 1) OVER w, 0) - coalesce(lag(tc, 2) OVER w, 0) AS trx_m1,
    tc - coalesce(lag(tc, 3) OVER w, 0) AS trx_3m
  FROM b
  WINDOW w AS (PARTITION BY client_id ORDER BY report_month)
)
SELECT client_id, CAST(report_month AS varchar) AS report_month, fold, midx, month(report_month) AS moy,
       t1, t2, t3, t4,
""" + ",\n".join(
    f"       t{k}_lag1, t{k}_lag2, t{k}_lag3, t{k}_cnt, midx - t{k}_last_idx AS t{k}_since" for k in range(1, 5)
) + """,
       t1_cnt + t2_cnt + t3_cnt + t4_cnt AS any_cnt,
       tc, dtd, trx_m, trx_m1, trx_3m,
       CASE WHEN tc > 0 THEN CAST(trx_m AS double) / tc END AS trx_share_m,
       CASE WHEN midx > 0 THEN CAST(tc AS double) / (midx + 1) END AS trx_per_month,
       CASE WHEN report_month < DATE '""" + VALID + """' THEN 'train'
            WHEN report_month < DATE '""" + TEST + """' THEN 'valid' ELSE 'test' END AS split
FROM f
"""

FEATURES = (["midx", "moy", "any_cnt", "tc", "dtd", "trx_m", "trx_m1", "trx_3m", "trx_share_m", "trx_per_month"]
            + [f"t{k}_{s}" for k in range(1, 5) for s in ("lag1", "lag2", "lag3", "cnt", "since")])

EDA_SQL = {
    "rate_by_month": """SELECT CAST(report_month AS varchar) m, """ + ", ".join(
        f"round(avg(CAST(target_{k} AS double)), 5) t{k}" for k in range(1, 5)) + """
        FROM silver_mbd.target_month GROUP BY 1 ORDER BY 1""",
}


def repeat_score(df) -> np.ndarray:
    """Luat hanh vi: mua thang truoc (x100) > so lan da mua (x10) > da mua 2-3 thang truoc."""
    cols = []
    for k in range(1, 5):
        cols.append(100 * df[f"t{k}_lag1"].fillna(0) + 10 * df[f"t{k}_cnt"].fillna(0)
                    + df[f"t{k}_lag2"].fillna(0) + df[f"t{k}_lag3"].fillna(0))
    return np.column_stack(cols).astype(float)


def eda(df) -> str:
    """Tóm tắt thăm dò trên chính bảng đặc trưng (không tốn thêm Athena)."""
    lines = ["### MBD-mini: thăm dò", ""]
    n, c = len(df), df.client_id.nunique()
    y = df[[f"t{k}" for k in range(1, 5)]]
    lines.append(f"- {n:,} dòng (khách × tháng), {c:,} khách, {df.report_month.nunique()} tháng báo cáo "
                 f"({df.report_month.min()} .. {df.report_month.max()}).")
    lines.append("- Tỉ lệ mua theo sản phẩm (mỗi khách-tháng): " + ", ".join(
        f"sp{k} {y[f't{k}'].mean():.3%}" for k in range(1, 5)) + f"; có ít nhất 1 sản phẩm: {(y.sum(1) > 0).mean():.2%}.")
    per_client = y.groupby(df.client_id).sum().sum(1)
    lines.append(f"- {(per_client > 0).mean():.1%} khách mua ít nhất một lần trong 12 tháng; trong số đó "
                 f"{(per_client > 1).sum() / max((per_client > 0).sum(), 1):.1%} mua từ 2 lần.")
    rows = []
    for k in range(1, 5):
        d = df[df[f"t{k}_lag1"].notna()]
        p1 = d.loc[d[f"t{k}_lag1"] == 1, f"t{k}"].mean()
        p0 = d.loc[d[f"t{k}_lag1"] == 0, f"t{k}"].mean()
        rows.append(f"sp{k}: {p1:.2%} nếu tháng trước có mua, {p0:.3%} nếu không (gấp {p1 / p0:,.0f} lần)")
    lines.append("- Tính lặp lại: " + "; ".join(rows) + ".")
    corr = y.corr().where(~np.eye(4, dtype=bool)).abs().max().max()
    lines.append(f"- Tương quan giữa nhãn các sản phẩm thấp (|r| lớn nhất {corr:.3f}): ít tín hiệu bán chéo trực tiếp.")
    z = (df.trx_m == 0).mean()
    lines.append(f"- {z:.1%} khách-tháng không có giao dịch mới; tỉ lệ mua của nhóm này "
                 f"{(y[df.trx_m == 0].sum(1) > 0).mean():.2%} so với {(y[df.trx_m > 0].sum(1) > 0).mean():.2%} ở nhóm có giao dịch.")
    return "\n".join(lines) + "\n"


def main() -> int:
    df = athena_df(FEATURE_SQL)
    df = df.rename(columns={f"t{k}": f"t{k}" for k in range(1, 5)})
    spec = Spec(name="MBD-mini (4 san pham, du doan thang sau)", products=PRODUCTS,
                label_cols=[f"t{k}" for k in range(1, 5)], feature_cols=FEATURES, repeat_score=repeat_score)
    res = fit(spec, df, neg_rate=0.25)
    out = pathlib.Path(__file__).resolve().parent / "reports"
    out.mkdir(exist_ok=True)
    md = eda(df) + "\n" + report(res)
    (out / "mbd.md").write_text(md, encoding="utf-8")
    (out / "mbd.json").write_text(json.dumps({k: v for k, v in res.items() if k != "models"}, indent=1, default=float),
                                  encoding="utf-8")
    for p, m in res["models"].items():
        m.save_model(str(out / f"mbd_{p}.lgb.txt"), num_iteration=m.best_iteration)
    print(md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
