"""Đặc trưng ĐỒ THỊ cho hệ gợi ý Berka (O10, docs/ONTOLOGY-BUILD.md; docs/RECSYS-V2.md mục 3.4).

Đọc thẳng từ knowledge graph (`track_a_onto.entity`, `triple`, `product_transition`) dưới persona data engineer,
không đọc lại silver: thí nghiệm đo giá trị của ĐỒ THỊ, nên đặc trưng phải đi qua đồ thị.

Đặc trưng tại (khách i, tháng t), chỉ dùng cạnh có `valid_from` < đầu tháng t+1 (chống rò rỉ thời gian):

  theo sản phẩm c (đưa vào mô hình multi-task dạng dài như `own_*`)
    own_g_district   tỉ lệ khách CÙNG QUẬN (Customer -locatedIn-> District) đang giữ c, bỏ chính i (leave-one-out)
    own_g_region     như trên ở mức VÙNG (District -partOf-> Region, bao đóng)
    own_g_cohold     1 nếu có người cùng tài khoản (holdsAccount / isAuthorizedOn) đang giữ c; NaN nếu không có ai
    own_g_partner    tỉ lệ "hàng xóm chuyển tiền" đang giữ c: khách khác chuyển tới CÙNG tài khoản đối tác
                     (Account -transfersTo-> Account, hai bước); đối tác quá đông (> HUB khách) bị bỏ vì không
                     mang thông tin cá nhân (vd. công ty điện nước)
    own_g_trans      max P(mua c trong 3 tháng | đang giữ A) từ `product_transition` (chỉ khoảng TRAIN)
  không theo sản phẩm
    g_cohold_deg     số người cùng tài khoản
    g_partner_deg    số hàng xóm chuyển tiền
"""

from __future__ import annotations

from typing import Dict, Tuple

import numpy as np
import pandas as pd

ONTO = "track_a_onto"
HUB = 30
IRI = {"fin:Loan": "loan", "fin:ClassicCard": "card_classic", "fin:JuniorCard": "card_junior",
       "fin:GoldCard": "card_gold", "fin:RecurringPayment": "household", "fin:InsuranceProduct": "insurance",
       "fin:CurrentAccount": "account"}
OWN = ("g_district", "g_region", "g_cohold", "g_partner", "g_trans")
GLOBAL = ("g_cohold_deg", "g_partner_deg")

_CLIENT = (f"JOIN {ONTO}.entity c ON c.uid = {{col}} AND c.class_iri = 'fin:Customer' AND c.source_system = 'berka'")
_CID = "CAST(regexp_extract(c.natural_key, 'client_id=([0-9]+)', 1) AS integer)"

SQL_HOLD = f"""
SELECT {_CID} AS client_id, regexp_replace(p.natural_key, '^iri=', '') AS iri, min(h.valid_from) AS acquired
FROM {ONTO}.triple h {_CLIENT.format(col='h.subject_uid')}
JOIN {ONTO}.triple o ON o.subject_uid = h.object_uid AND o.predicate_iri = 'fin:ofProduct'
JOIN {ONTO}.entity p ON p.uid = o.object_uid
WHERE h.predicate_iri = 'fin:hasHolding' AND h.source_dataset LIKE 'silver_berka%'
GROUP BY 1, 2
"""

SQL_LOC = f"""
SELECT DISTINCT {_CID} AS client_id, d.label AS district, r.label AS region
FROM {ONTO}.triple l {_CLIENT.format(col='l.subject_uid')}
JOIN {ONTO}.entity d ON d.uid = l.object_uid AND d.class_iri = 'fin:District'
LEFT JOIN (SELECT k.start_uid, r.label FROM {ONTO}.closure k
           JOIN {ONTO}.entity r ON r.uid = k.node_uid AND r.class_iri = 'fin:Region'
           WHERE k.predicate_iri = 'fin:partOf') r ON r.start_uid = d.uid
WHERE l.predicate_iri = 'fin:locatedIn' AND l.source_dataset LIKE 'silver_berka%'
"""

SQL_ACC = f"""
SELECT {_CID} AS client_id, a.object_uid AS account, min(a.valid_from) AS since
FROM {ONTO}.triple a {_CLIENT.format(col='a.subject_uid')}
WHERE a.predicate_iri IN ('fin:holdsAccount', 'fin:isAuthorizedOn') AND a.source_dataset LIKE 'silver_berka%'
GROUP BY 1, 2
"""

SQL_PARTNER = f"""
SELECT {_CID} AS client_id, t.object_uid AS partner, min(t.valid_from) AS since, count(*) AS n
FROM {ONTO}.triple t
JOIN {ONTO}.triple a ON a.object_uid = t.subject_uid AND a.predicate_iri = 'fin:holdsAccount'
     AND a.source_dataset LIKE 'silver_berka%'
{_CLIENT.format(col='a.subject_uid')}
WHERE t.predicate_iri = 'fin:transfersTo' AND t.source_dataset LIKE 'silver_berka%'
GROUP BY 1, 2
"""

SQL_TRANS = f"""
SELECT from_product, to_product, p, n_from, lift, train_cutoffs
FROM {ONTO}.product_transition
WHERE horizon_months = 3 AND contains(sources, 'corebank') AND n_from >= 30
"""


def fetch(use_cache: bool = True) -> Dict[str, pd.DataFrame]:
    """Năm bảng cạnh từ đồ thị (cache theo băm câu SQL, như recsys.data.athena_df)."""
    from recsys.data import athena_df

    out = {k: athena_df(s, use_cache=use_cache) for k, s in
           (("hold", SQL_HOLD), ("loc", SQL_LOC), ("acc", SQL_ACC), ("partner", SQL_PARTNER), ("trans", SQL_TRANS))}
    out["hold"]["code"] = out["hold"]["iri"].map(IRI)
    for k, c in (("hold", "acquired"), ("acc", "since"), ("partner", "since")):
        out[k][c] = pd.to_datetime(out[k][c])
    return out


# ---------------------------------------------------------------------------------------------- tính đặc trưng
def _pairs(links: pd.DataFrame, key: str, hub: int = 0) -> pd.DataFrame:
    """(i, j, since) cho mọi cặp khách khác nhau cùng `key`; since = thời điểm cả hai đã nối vào key."""
    if hub:
        size = links.groupby(key)["client_id"].transform("nunique")
        links = links[size <= hub]
    m = links.merge(links, on=key, suffixes=("_i", "_j"))
    m = m[m.client_id_i != m.client_id_j]
    m["since"] = m[["since_i", "since_j"]].max(axis=1)
    return (m.groupby(["client_id_i", "client_id_j"], as_index=False)["since"].min()
            .rename(columns={"client_id_i": "i", "client_id_j": "j"}))


def build(df: pd.DataFrame, g: Dict[str, pd.DataFrame], codes, id_col: str = "client_id",
          month_col: str = "month", relative: bool = True) -> Tuple[pd.DataFrame, Dict[str, Dict[str, str]]]:
    """Thêm cột đặc trưng đồ thị vào `df` (khách × tháng). Trả (df mới, own mapping {code: {own_g_*: cột}}).

    relative=True: tỉ lệ nhóm (quận, vùng, hàng xóm) CHIA cho tỉ lệ toàn ngân hàng cùng tháng (= lift của nhóm).
    Tỉ lệ thô là đại lượng cộng dồn tăng theo thời gian: ở tháng test nó nằm ngoài khoảng đã thấy khi train, mô
    hình học "thời gian" thay vì "nhóm" (đo 11-22: tỉ lệ thô làm MAP@3 GIẢM, xem reports/berka/graph_ablation.md)."""
    df = df.copy()
    month = pd.to_datetime(df[month_col])
    ends = np.sort(month.unique())
    ids = np.sort(pd.unique(pd.concat([df[id_col], g["hold"].client_id, g["loc"].client_id])))
    pos = pd.Series(np.arange(len(ids)), index=ids)
    n, K = len(ids), len(codes)
    acq = np.full((n, K), np.datetime64("NaT"), dtype="datetime64[ns]")
    h = g["hold"][g["hold"].code.isin(codes)]
    for c_idx, c in enumerate(codes):
        hc = h[h.code == c]
        acq[pos[hc.client_id].to_numpy(), c_idx] = hc.acquired.to_numpy()
    start = np.full(n, np.datetime64("NaT"), dtype="datetime64[ns]")     # khách "tồn tại" từ tài khoản đầu tiên
    a0 = g["acc"].groupby("client_id").since.min()
    start[pos[a0.index].to_numpy()] = a0.to_numpy()
    loc = g["loc"].drop_duplicates("client_id").set_index("client_id").reindex(ids)
    dist = loc["district"].astype("category").cat.codes.to_numpy()
    reg = loc["region"].astype("category").cat.codes.to_numpy()
    cohold = _pairs(g["acc"].rename(columns={"account": "key"}), "key")
    partner = _pairs(g["partner"].rename(columns={"partner": "key"}), "key", hub=HUB)
    # product_transition: P(c | A) cho A đang giữ (mã nguồn theo IRI)
    tr = g["trans"].assign(f=g["trans"].from_product.map(IRI), t=g["trans"].to_product.map(IRI)).dropna(subset=["f", "t"])
    held_codes = sorted(set(tr.f))
    hold_all = g["hold"].assign(code=g["hold"].code)
    acq_from = {a: pd.Series(hold_all[hold_all.code == a].set_index("client_id").acquired) for a in held_codes}

    feats = {f"{o}_{c}": np.full(len(df), np.nan) for o in OWN for c in codes}
    feats.update({gname: np.full(len(df), np.nan) for gname in GLOBAL})
    row_pos = pos[df[id_col]].to_numpy()

    def group_rate(grp, H, alive):
        """leave-one-out tỉ lệ giữ c trong nhóm, cho mọi khách."""
        G = grp.max() + 2
        g_ = np.where(grp < 0, G - 1, grp)
        n_alive = np.bincount(g_, weights=alive, minlength=G)
        out = np.full((n, K), np.nan)
        for k in range(K):
            n_h = np.bincount(g_, weights=H[:, k] * alive, minlength=G)
            den = n_alive[g_] - alive
            out[:, k] = np.where(den > 0, (n_h[g_] - H[:, k] * alive) / np.maximum(den, 1), np.nan)
        return out

    def neighbor(pairs, H, end):
        p = pairs[pairs.since < end]
        i, j = pos[p.i].to_numpy(), pos[p.j].to_numpy()
        deg = np.bincount(i, minlength=n).astype(float)
        hs = np.zeros((n, K))
        for k in range(K):
            hs[:, k] = np.bincount(i, weights=H[j, k], minlength=n)
        return deg, hs

    for m in ends:
        end = (pd.Timestamp(m) + pd.offsets.MonthBegin(1)).to_datetime64()
        rows = np.flatnonzero(month.to_numpy() == m)
        ri = row_pos[rows]
        H = (acq < end).astype(float)              # NaT < end là False
        alive = (start < end).astype(float)
        D, R = group_rate(dist, H, alive), group_rate(reg, H, alive)
        cdeg, chs = neighbor(cohold, H, end)
        pdeg, phs = neighbor(partner, H, end)
        prate = phs / np.maximum(pdeg, 1)[:, None]
        if relative:
            base = (H * alive[:, None]).sum(0) / max(alive.sum(), 1)           # tỉ lệ toàn ngân hàng tháng này
            base = np.where(base > 0, base, np.nan)
            D, R, prate = D / base, R / base, prate / base
        best = np.full((n, K), np.nan)
        for a in held_codes:
            s = acq_from[a]
            has_a = np.zeros(n, bool)
            has_a[pos[s.index[s.to_numpy() < end]].to_numpy()] = True
            for k, c in enumerate(codes):
                v = tr[(tr.f == a) & (tr.t == c)].p
                if len(v):
                    best[has_a, k] = np.fmax(best[has_a, k], float(v.iloc[0]))
        for k, c in enumerate(codes):
            feats[f"g_district_{c}"][rows] = D[ri, k]
            feats[f"g_region_{c}"][rows] = R[ri, k]
            feats[f"g_cohold_{c}"][rows] = np.where(cdeg[ri] > 0, (chs[ri, k] > 0).astype(float), np.nan)
            feats[f"g_partner_{c}"][rows] = np.where(pdeg[ri] > 0, prate[ri, k], np.nan)
            feats[f"g_trans_{c}"][rows] = best[ri, k]
        feats["g_cohold_deg"][rows] = cdeg[ri]
        feats["g_partner_deg"][rows] = pdeg[ri]
    for k, v in feats.items():
        df[k] = v
    own = {c: {f"own_{o}": f"{o}_{c}" for o in OWN} for c in codes}
    return df, own


def with_graph(ds, g=None, relative: bool = True):
    """Dataset Berka + đặc trưng đồ thị (bản 'có đồ thị' của thí nghiệm O10)."""
    from dataclasses import replace

    g = g if g is not None else fetch()
    df, own = build(ds.df, g, ds.domain.codes, ds.id_col, ds.month_col, relative=relative)
    merged = {c: {**ds.own.get(c, {}), **own[c]} for c in ds.domain.codes}
    return replace(ds, df=df, features=list(ds.features) + list(GLOBAL), own=merged)
