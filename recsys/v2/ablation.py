"""Thí nghiệm O10: đặc trưng ĐỒ THỊ có giúp mô hình gợi ý không (docs/ONTOLOGY-BUILD.md mục O10, RECSYS-V2.md 3.4).

    python -m recsys.v2.ablation            # Berka, ~2 phút; ghi recsys/v2/reports/berka/graph_ablation.{md,json}

Thiết kế đối chứng: cùng tập test THỜI GIAN, cùng mô hình champion (LightGBM multi-task + isotonic + chọn cách xếp
trên valid), cùng tham số; chỉ khác có / không có đặc trưng đồ thị (recsys/v2/graph.py). Lặp N_SEEDS seed để thấy
phương sai huấn luyện; bootstrap ghép cặp (seed 0) cho CI 95% của chênh lệch; bỏ từng nhóm đặc trưng (seed 0) để
biết nhóm nào đóng góp.
"""

from __future__ import annotations

import json
import sys
import time
from dataclasses import replace

import lightgbm  # noqa: F401  (import trước numpy/pandas trên Windows)
import numpy as np

from recsys import metrics as M
from recsys.metrics import table

from . import evaluate as EV
from . import graph as G
from .data import load, matrices
from .model import Ranker
from .run import REPORTS

N_SEEDS = 3


def _drop(ds, group: str):
    """Bỏ một nhóm đặc trưng đồ thị khỏi Dataset (own_<group> và cột toàn cục cùng tên)."""
    own = {c: {k: v for k, v in m.items() if k != f"own_{group}"} for c, m in ds.own.items()}
    feats = [f for f in ds.features if not f.startswith(group)]
    return replace(ds, own=own, features=feats)


def _fit_eval(ds, seed: int):
    m = Ranker.fit(ds, log=lambda *_: None, params={"seed": seed})
    te = ds.split("test")
    Y, E = matrices(ds, te)
    R, P = m.rank_scores(ds, te), m.predict(ds, te)
    rk = EV.ranking(Y, R, E)
    pp = {c: M.per_product(Y[E[:, j], j], P[E[:, j], j]) for j, c in enumerate(ds.domain.codes)}
    return m, (Y, E, R), {"ranking": rk, "per_product": pp, "rank_with": m.meta["rank_with"],
                          "best_iter": m.meta["best_iter"], "valid_ap": m.meta["valid_ap"]}


VARIANTS = ("no_graph", "graph_raw", "graph_rel")
LABEL = {"no_graph": "Không đồ thị", "graph_raw": "Đồ thị (tỉ lệ thô)", "graph_rel": "Đồ thị (chuẩn hoá theo tháng)"}


def run(log=print) -> dict:
    t0 = time.time()
    base = load("berka")
    g = G.fetch()
    dsv = {"no_graph": base, "graph_raw": G.with_graph(base, g, relative=False), "graph_rel": G.with_graph(base, g)}
    graph = dsv["graph_rel"]
    te = graph.split("test")
    cols = [c for c in graph.df.columns if c.startswith("g_")]
    cover = {c: float(te[c].notna().mean()) for c in cols}
    # trôi: PSI train -> test của từng đặc trưng đồ thị, bản thô và bản chuẩn hoá
    psi = {}
    for v in ("graph_raw", "graph_rel"):
        d = dsv[v].df
        tr_, te_ = d[d.split == "train"], d[d.split == "test"]
        psi[v] = {c: EV.psi(tr_[c].dropna().to_numpy(), te_[c].dropna().to_numpy())
                  for c in cols if tr_[c].notna().sum() > 100 and te_[c].notna().sum() > 100}
    # kiểm tra rò rỉ: không đặc trưng đồ thị nào tương quan quá mạnh với nhãn của chính sản phẩm đó
    leak = {}
    for c in graph.domain.codes:
        y = te[graph.domain.label_prefix + c]
        for o in G.OWN:
            x = te[f"{o}_{c}"]
            ok = x.notna() & te[f"elig_{c}"]
            if ok.sum() > 100 and x[ok].std() > 0:
                leak[f"{o}_{c}"] = float(np.corrcoef(x[ok], y[ok])[0, 1])
    res = {"domain": "berka", "n_test": len(te), "test_months": sorted(map(str, set(te["month"]))),
           "graph_features": cols, "coverage_test": cover, "psi": psi, "corr_with_label": leak,
           "partner_shared": int((g["partner"].groupby("partner").client_id.nunique() > 1).sum()),
           "partner_total": int(g["partner"].partner.nunique()), "seeds": []}
    for s in range(N_SEEDS):
        row, R = {"seed": s}, {}
        for v in VARIANTS:
            log(f"  seed {s}: {v} ...")
            m, (Y, E, R[v]), row[v] = _fit_eval(dsv[v], s)
            if s == 0 and v == "graph_rel":
                imp = m.booster.feature_importance("gain")
                imp = imp / max(imp.sum(), 1e-9)
                res["importance_graph_model"] = {k: float(x) for k, x in sorted(zip(m.fcols, imp), key=lambda kv: -kv[1])}
                res["graph_gain_share"] = float(sum(x for k, x in zip(m.fcols, imp) if "g_" in k))
        if s == 0:
            res["bootstrap"] = {v: EV.bootstrap_diff(Y, R[v], R["no_graph"], E) for v in ("graph_raw", "graph_rel")}
        res["seeds"].append(row)
    log("  bo tung nhom (seed 0, ban chuan hoa) ...")
    res["drop_one"] = []
    for grp in G.OWN + ("g_cohold_deg", "g_partner_deg"):
        _, _, r = _fit_eval(_drop(graph, grp), 0)
        res["drop_one"].append({"dropped": grp, **{k: r["ranking"][k] for k in ("hit@1", "map@3", "ndcg@3")}})
    res["only_one"] = []
    for grp in G.OWN:
        keep = [o for o in G.OWN + ("g_cohold_deg", "g_partner_deg") if o != grp]
        d = graph
        for o in keep:
            d = _drop(d, o)
        _, _, r = _fit_eval(d, 0)
        res["only_one"].append({"only": grp, **{k: r["ranking"][k] for k in ("hit@1", "map@3", "ndcg@3")}})
    res["seconds"] = round(time.time() - t0, 1)
    out = REPORTS / "berka"
    out.mkdir(parents=True, exist_ok=True)
    (out / "graph_ablation.json").write_text(json.dumps(res, ensure_ascii=False, indent=1, default=float), encoding="utf-8")
    (out / "graph_ablation.md").write_text(to_md(res), encoding="utf-8")
    log(f"  bao cao {out / 'graph_ablation.md'} [{res['seconds']}s]")
    return res


def _ms(vals):
    return f"{np.mean(vals):.4f} ± {np.std(vals):.4f}"


def to_md(r: dict) -> str:
    S = r["seeds"]
    V = VARIANTS
    L = [f"# O10: đặc trưng đồ thị cho hệ gợi ý Berka ({time.strftime('%Y-%m-%d')})", "",
         f"Tập test {r['n_test']:,} dòng (khách × tháng), tháng {r['test_months'][0]} .. {r['test_months'][-1]}. "
         "Cùng mô hình champion, cùng tham số, chỉ khác đặc trưng. "
         f"Sinh bởi `python -m recsys.v2.ablation`; {len(S)} seed (trung bình ± độ lệch chuẩn).", "",
         "## 1. Xếp hạng theo khách (test)", "",
         "| Chỉ số | " + " | ".join(LABEL[v] for v in V) + " |", "|---|" + "---:|" * len(V)]
    for k in ("hit@1", "mrr", "map@3", "ndcg@3", "recall@3"):
        L.append(f"| {k} | " + " | ".join(_ms([s[v]["ranking"][k] for s in S]) for v in V) + " |")
    L += ["", "Chênh lệch so với không đồ thị (seed 0, bootstrap ghép cặp 1000 lần, CI 95%):", ""]
    for v, b in r["bootstrap"].items():
        L.append(f"- {LABEL[v]}: " + "; ".join(f"Δ {k} {x[0]:+.4f} [{x[1]:+.4f}; {x[2]:+.4f}]" for k, x in b.items()))
    L += ["", "## 2. Nhắm khách theo sản phẩm (PR-AUC, test)", "",
          "| Sản phẩm | Ca dương | " + " | ".join(LABEL[v] for v in V) + " |", "|---|---:|" + "---:|" * len(V)]
    for c in S[0]["no_graph"]["per_product"]:
        L.append(f"| {c} | {S[0]['no_graph']['per_product'][c]['pos']} | "
                 + " | ".join(_ms([s[v]["per_product"][c]["pr_auc"] for s in S]) for v in V) + " |")
    L += ["", "## 3. Đóng góp từng nhóm (bản chuẩn hoá, seed 0)", "",
          "Bỏ một nhóm (các nhóm khác giữ nguyên):", "",
          table(r["drop_one"], ["dropped", "hit@1", "map@3", "ndcg@3"]),
          "Chỉ giữ một nhóm (cộng vào đặc trưng gốc):", "",
          table(r["only_one"], ["only", "hit@1", "map@3", "ndcg@3"]),
          f"Tỉ trọng gain của đặc trưng đồ thị (bản chuẩn hoá): {r['graph_gain_share']:.1%}. Mười đặc trưng đứng đầu: "
          + ", ".join(f"`{k}` {v:.3f}" for k, v in list(r["importance_graph_model"].items())[:10]) + ".", "",
          "## 4. Trôi theo thời gian, độ phủ, rò rỉ", "",
          "| Đặc trưng | Có giá trị (test) | PSI train→test, thô | PSI train→test, chuẩn hoá |", "|---|---:|---:|---:|"]
    for k, v in r["coverage_test"].items():
        a, b = r["psi"]["graph_raw"].get(k), r["psi"]["graph_rel"].get(k)
        L.append(f"| `{k}` | {v:.1%} | {'-' if a is None else f'{a:.3f}'} | {'-' if b is None else f'{b:.3f}'} |")
    mx = max(r["corr_with_label"].items(), key=lambda kv: abs(kv[1])) if r["corr_with_label"] else ("-", 0.0)
    L += ["", "PSI > 0,25 là trôi đáng kể.",
          f"Tương quan lớn nhất giữa đặc trưng đồ thị và nhãn của chính sản phẩm: `{mx[0]}` {mx[1]:+.3f} "
          "(ngưỡng nghi rò rỉ 0,5).",
          f"Đồ thị chuyển tiền: {r['partner_shared']} / {r['partner_total']:,} tài khoản đối tác có từ 2 khách trở lên "
          "(Berka ẩn danh tài khoản ngoài ngân hàng, nên gần như không có hàng xóm hai bước).", ""]
    return "\n".join(L)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    run()
