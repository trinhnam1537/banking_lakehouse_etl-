"""Gợi ý sản phẩm tiếp theo trên Berka (UC2): 4.500 khách, 1993–1998, 6 sản phẩm, nhãn = mua sản phẩm CHƯA giữ trong
3 tháng sau tháng chụp. Bảng nguồn track_a_pretrain.uc2_next_product (transforms/pretrain/uc2_next_product.py: đã
chống rò rỉ, chia theo thời gian train < 1997-07 <= valid < 1998-01 <= test).

    python -m recsys.berka          # ghi recsys/reports/berka.md

Bảng nguồn ở dạng DÀI (một dòng mỗi sản phẩm chưa giữ); ở đây gộp về dạng RỘNG (khách, tháng) để xếp hạng 6 sản phẩm
cho từng khách. eligible_<p> = sản phẩm còn được gợi ý (khách chưa giữ tại tháng chụp).

Đối chứng thay cho "repeat" (mỗi sản phẩm chỉ mua một lần): luật kết hợp bán chéo P(mua p | đang giữ q) học trên train.
"""

from __future__ import annotations

import json
import pathlib

import numpy as np

from .data import athena_df
from .trainer import Spec, fit, report

PRODUCTS = ["household", "loan", "card_classic", "insurance", "card_junior", "card_gold"]
NUM = ["n_accounts", "txn_cnt", "inflow", "outflow", "end_balance", "cash_wd_cnt", "card_wd_cnt", "avg_balance",
       "min_balance", "txn_cnt_3m", "inflow_3m", "outflow_3m", "cash_wd_cnt_3m", "card_wd_cnt_3m", "min_balance_3m",
       "avg_balance_3m", "pension_3m", "overdraft_3m", "inflow_trend", "net_flow_3m", "tenure_months",
       "district_avg_salary", "district_unemployment_96", "district_urban_ratio", "district_inhabitants",
       "district_entrepreneurs_per_1000", "age", "n_products"]
HAS = [f"has_{p}" for p in PRODUCTS]
FEATURES = NUM + HAS + ["is_male", "region_code", "moy"]

SQL = """
SELECT client_id, CAST(month AS varchar) AS month, split,
""" + ",\n".join(f"       arbitrary({c}) AS {c}" for c in NUM) + ",\n" + ",\n".join(
    f"       CAST(arbitrary(has_{p}) AS integer) AS has_{p}" for p in PRODUCTS) + """,
       CASE WHEN arbitrary(gender) = 'M' THEN 1 ELSE 0 END AS is_male,
       arbitrary(region) AS region, month(month) AS moy,
""" + ",\n".join(
    f"       coalesce(max(CASE WHEN product = '{p}' THEN CAST(label AS integer) END), 0) AS y_{p},\n"
    f"       count_if(product = '{p}') > 0 AS elig_{p}" for p in PRODUCTS) + """
FROM track_a_pretrain.uc2_next_product
GROUP BY client_id, month, split
"""


def cooccurrence_score(train):
    """f(df) -> diem (n, 6): max_q P(mua p | giu q) tren train, mac dinh ti le mua p cua khach khong giu gi."""
    base = {p: train.loc[train[f"elig_{p}"], f"y_{p}"].mean() for p in PRODUCTS}
    cond = {}
    for p in PRODUCTS:
        for q in PRODUCTS:
            if p == q:
                continue
            m = train[f"elig_{p}"] & (train[f"has_{q}"] == 1)
            cond[(p, q)] = train.loc[m, f"y_{p}"].mean() if m.sum() >= 50 else np.nan

    def score(df):
        S = np.zeros((len(df), len(PRODUCTS)))
        for j, p in enumerate(PRODUCTS):
            s = np.full(len(df), base[p])
            for q in PRODUCTS:
                c = cond.get((p, q))
                if c is not None and not np.isnan(c):
                    s = np.where(df[f"has_{q}"].to_numpy() == 1, np.maximum(s, c), s)
            S[:, j] = s
        return S
    return score


def eda(df) -> str:
    lines = ["### Berka UC2: thăm dò", ""]
    lines.append(f"- {len(df):,} dòng (khách × tháng chụp), {df.client_id.nunique():,} khách, tháng "
                 f"{df.month.min()} .. {df.month.max()}.")
    for s in ("train", "valid", "test"):
        d = df[df.split == s]
        rates = ", ".join(f"{p} {d.loc[d[f'elig_{p}'], f'y_{p}'].mean():.2%}" for p in PRODUCTS)
        lines.append(f"- {s}: {len(d):,} dòng; tỉ lệ mua trong 3 tháng (trên khách chưa giữ): {rates}.")
    held = df[HAS].mean()
    lines.append("- Tỉ lệ khách-tháng đang giữ: " + ", ".join(f"{p} {held[f'has_{p}']:.1%}" for p in PRODUCTS) + ".")
    lines.append(f"- Số ca dương trên tập train ít ở thẻ gold ({int(df.loc[df.split == 'train', 'y_card_gold'].sum())}) "
                 f"và thẻ junior ({int(df.loc[df.split == 'train', 'y_card_junior'].sum())}): mô hình cho hai sản phẩm "
                 "này có phương sai cao. Nhãn 3 tháng của cùng một lần mua lặp ở 3 tháng chụp liên tiếp.")
    return "\n".join(lines) + "\n"


def run_variant(df, name: str):
    spec = Spec(name=name, products=PRODUCTS, label_cols=[f"y_{p}" for p in PRODUCTS], feature_cols=FEATURES,
                categorical=["region_code"], eligible_cols=[f"elig_{p}" for p in PRODUCTS],
                repeat_score=cooccurrence_score(df[df.split == "train"]))
    res = fit(spec, df)
    for r in res["ranking"] + res["per_product"]:
        if r["model"] == "repeat":
            r["model"] = "cooccurrence"
    return res


def tenure_table(df) -> str:
    """Bằng chứng artefact giai đoạn mở tài khoản: tỉ lệ mua theo tenure (tập test, khách chưa giữ)."""
    import pandas as pd

    t = df[df.split == "test"].copy()
    t["tenure"] = pd.cut(t.tenure_months, [0, 3, 6, 12, 24, 60, 999],
                         labels=["1-3", "4-6", "7-12", "13-24", "25-60", ">60"])
    rows = []
    for p in PRODUCTS:
        d = t[t[f"elig_{p}"]]
        g = d.groupby("tenure", observed=False)[f"y_{p}"].mean()
        rows.append({"product": p, **{str(k): f"{v:.2%}" for k, v in g.items()}})
    from .metrics import table

    return ("**Tỉ lệ mua trong 3 tháng theo tenure (tháng), tập test:**\n\n"
            + table(rows, ["product", "1-3", "4-6", "7-12", "13-24", "25-60", ">60"]))


def main() -> int:
    df = athena_df(SQL)
    df["region_code"] = df["region"].astype("category").cat.codes
    for p in PRODUCTS:
        df[f"elig_{p}"] = df[f"elig_{p}"].astype(bool)
    out = pathlib.Path(__file__).resolve().parent / "reports"
    out.mkdir(exist_ok=True)
    res_all = run_variant(df, "Berka UC2, moi khach (6 san pham, mua trong 3 thang)")
    est = df[df.tenure_months > 6].copy()
    res_est = run_variant(est, "Berka UC2, khach on dinh (tenure > 6 thang)")
    md = eda(df) + "\n" + tenure_table(df) + "\n" + report(res_all) + "\n" + report(res_est)
    (out / "berka.md").write_text(md, encoding="utf-8")
    (out / "berka.json").write_text(json.dumps({"all": {k: v for k, v in res_all.items() if k != "models"},
                                                "established": {k: v for k, v in res_est.items() if k != "models"}},
                                               indent=1, default=float), encoding="utf-8")
    print(md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
