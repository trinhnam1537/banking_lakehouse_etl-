"""Điều phối hệ gợi ý v2. Xem recsys/v2/__init__.py và docs/RECSYS-V2.md.

    python -m recsys.v2.run all --domain mbd        # train + evaluate + online
    python -m recsys.v2.run all --domain berka      # train + evaluate + score
    python -m recsys.v2.run publish --domain berka  # đưa reco_topk lên track_a_gold (persona analyst)
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
import time

import lightgbm  # noqa: F401
import numpy as np

from recsys.metrics import table

from . import evaluate as EV
from . import online as ON
from . import serve as SV
from .data import Dataset, load, matrices
from .model import Ranker

HERE = pathlib.Path(__file__).resolve().parent
REGISTRY = HERE / "registry"
REPORTS = HERE / "reports"
EVENTS = HERE / "events"


def _f(v, nd=4):
    return f"{v:.{nd}f}" if isinstance(v, float) else str(v)


# ---------------------------------------------------------------------------------------------- đối chứng
def baselines(ds: Dataset, te, log=print) -> dict:
    tr = ds.split("train")
    Ytr, Etr = matrices(ds, tr)
    pop = Ytr.sum(0) / np.maximum(Etr.sum(0), 1)
    out = {"popularity": np.tile(pop, (len(te), 1))}
    if ds.domain.name == "mbd":
        from recsys import mbd
        from recsys.bench import models as BM

        out["repeat"] = mbd.repeat_score(te) + pop * 1e-3
        log("  doi chung v1: LightGBM moi san pham (dac trung goc) ...")
        _, St, _ = BM.lgb_per_product(tr, ds.split("valid"), te, list(mbd.FEATURES), log=lambda *_: None, tag="v1")
        out["v1_lgb_per_product"] = St
    else:
        from recsys import berka

        sc = berka.cooccurrence_score(tr)
        S = sc(te)
        idx = [berka.PRODUCTS.index(c) for c in ds.domain.codes]
        out["cooccurrence"] = S[:, idx] + pop * 1e-3
    return out


# ---------------------------------------------------------------------------------------------- train
def cmd_train(ds: Dataset, log=print) -> Ranker:
    m = Ranker.fit(ds, log=log)
    version = time.strftime("v2-%Y%m%d-%H%M")
    m.meta["version"] = version
    path = m.save(REGISTRY, version)
    log(f"  luu {path}")
    return m


# ---------------------------------------------------------------------------------------------- evaluate
def cmd_evaluate(ds: Dataset, m: Ranker, log=print) -> dict:
    dom = ds.domain
    te = ds.split("test")
    Y, E = matrices(ds, te)
    P = m.predict(ds, te)
    Praw = m.predict(ds, te, calibrated=False)
    R = m.rank_scores(ds, te)
    base = baselines(ds, te, log)
    other = "raw" if m.meta.get("rank_with") == "calibrated" else "calibrated"
    models = {f"v2 (xep theo {m.meta.get('rank_with')}, champion)": R,
              f"v2 (xep theo {other}, so sanh)": Praw if other == "raw" else P, **base}
    res = {"domain": dom.name, "model_version": m.meta.get("version"), "n_test": len(te),
           "test_months": sorted(map(str, set(te["month"])))}
    res["ranking"] = [{"model": k, **EV.ranking(Y, S, E)} for k, S in models.items()]
    ref = "v1_lgb_per_product" if "v1_lgb_per_product" in models else "popularity"
    res["bootstrap_vs"] = ref
    res["bootstrap"] = {k: EV.bootstrap_diff(Y, S, models[ref], E) for k, S in models.items() if k != ref}
    res["per_product"] = EV.per_product(Y, P, E, dom.codes)
    res["calibration"] = {}
    for j, c in enumerate(dom.codes):
        s = E[:, j]
        res["calibration"][c] = {"before": EV.calibration(Y[s, j], Praw[s, j]), "after": EV.calibration(Y[s, j], P[s, j])}
    ever = None
    if dom.name == "mbd":
        ever = np.column_stack([(te[f"t{c}_cnt"].fillna(0) > 0).to_numpy() for c in dom.codes]).astype(int)
    res["beyond"] = {k: EV.beyond_accuracy(S, E, ever) for k, S in models.items()}
    res["segments"] = {name: EV.by_segment(te[col].astype(str).to_numpy(), Y, R, E) for name, col in ds.segments.items()}
    res["by_month"] = EV.by_month(te["month"].astype(str).to_numpy(), Y, R, E)
    tr = ds.split("train").sample(min(len(ds.split("train")), 200000), random_state=0)
    Ptr = m.predict(ds, tr)
    Etr = matrices(ds, tr)[1]
    res["psi"] = {c: EV.psi(Ptr[Etr[:, j], j], P[E[:, j], j]) for j, c in enumerate(dom.codes)}
    res["campaign"] = {}
    for j, p in enumerate(dom.products):
        s = E[:, j]
        res["campaign"][p.code] = EV.campaign(Y[s, j], P[s, j], p.value, dom.contact_cost)
    pop = Y.sum(0) / np.maximum(E.sum(0), 1)
    target = np.argmax(np.where(E, R, -np.inf), 1)
    res["off_policy"] = EV.off_policy(Y, E, target, P, pop)
    imp = m.booster.feature_importance("gain")
    res["importance"] = dict(sorted(zip(m.fcols, (imp / max(imp.sum(), 1e-9)).round(4).tolist()), key=lambda kv: -kv[1])[:15])
    out = REPORTS / dom.name
    out.mkdir(parents=True, exist_ok=True)
    (out / "offline.json").write_text(json.dumps(res, ensure_ascii=False, indent=1, default=float), encoding="utf-8")
    (out / "offline.md").write_text(offline_md(res, dom), encoding="utf-8")
    log(f"  bao cao {out / 'offline.md'}")
    return res


def offline_md(r: dict, dom) -> str:
    L = [f"# Đánh giá offline hệ gợi ý v2: {dom.name} ({time.strftime('%Y-%m-%d')})", "",
         f"Mô hình `{r['model_version']}`; tập test {r['n_test']:,} dòng (khách × tháng), tháng {', '.join(r['test_months'])}.",
         "Sinh bởi `python -m recsys.v2.run evaluate`. Giải thích từng nhóm chỉ số: docs/RECSYS-V2.md mục 4.", "",
         "## 1. Độ chính xác xếp hạng (từng khách, dòng có ít nhất 1 sản phẩm được mua)", "",
         table(sorted(r["ranking"], key=lambda x: -x.get("map@3", 0)),
               ["model", "rows", "hit@1", "mrr", "map@3", "ndcg@3", "recall@3"]),
         f"**Bootstrap ghép cặp 1000 lần, chênh lệch so với `{r['bootstrap_vs']}` (CI 95%):**", "",
         "| Mô hình | Δ Hit@1 | Δ MAP@3 [CI 95%] | Δ NDCG@3 |", "|---|---:|---|---:|"]
    for k, b in r["bootstrap"].items():
        L.append(f"| {k} | {b['hit@1'][0]:+.4f} | {b['map@3'][0]:+.4f} [{b['map@3'][1]:+.4f}; {b['map@3'][2]:+.4f}] | "
                 f"{b['ndcg@3'][0]:+.4f} |")
    L += ["", "## 2. Nhắm khách theo sản phẩm (v2)", "",
          table(r["per_product"], ["product", "pos", "rate", "roc_auc", "pr_auc", "lift@1%", "recall@5%"]),
          "## 3. Hiệu chỉnh xác suất (trước / sau isotonic)", "",
          "| Sản phẩm | Tỉ lệ thật | p TB trước | p TB sau | Brier trước | Brier sau | Brier tham chiếu | ECE trước | ECE sau |",
          "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for c, v in r["calibration"].items():
        b, a = v["before"], v["after"]
        L.append(f"| {c} | {a['rate']:.4%} | {b['mean_p']:.4%} | {a['mean_p']:.4%} | {b['brier']:.5f} | {a['brier']:.5f} | "
                 f"{a['brier_ref']:.5f} | {b['ece']:.5f} | {a['ece']:.5f} |")
    L += ["", "Brier tham chiếu = dự đoán hằng số bằng tỉ lệ nền. ECE tính theo 10 bin phân vị.", "",
          "## 4. Ngoài độ chính xác (gợi ý top-1)", "",
          "| Mô hình | Độ phủ | Gini hiển thị | Độ mới | Cá nhân hoá | Tỉ trọng hiển thị theo sản phẩm |", "|---|---:|---:|---:|---:|---|"]
    for k, v in r["beyond"].items():
        L.append(f"| {k} | {v['coverage']:.2f} | {v['gini']:.3f} | {_f(v.get('novelty_top1', '-'), 3)} | "
                 f"{v['personalization']:.3f} | {', '.join(f'{x:.0%}' for x in v['exposure_share'])} |")
    L += ["", "## 5. Phân khúc (v2)", ""]
    for name, s in r["segments"].items():
        L += [f"**{name}** (tỉ số MAP@3 xấu nhất / tốt nhất = {_f(s['worst_best_ratio'], 3)}):", "",
              table(s["rows"], ["segment", "rows", "hit@1", "map@3", "ndcg@3"])]
    L += ["## 6. Ổn định theo thời gian", "", table(r["by_month"], ["month", "rows", "hit@1", "map@3", "ndcg@3"]),
          "PSI phân bố điểm train -> test: " + ", ".join(f"{k} {v:.3f}" for k, v in r["psi"].items())
          + " (< 0,1 ổn định; 0,1-0,25 theo dõi; > 0,25 trôi).", "",
          f"## 7. Giá trị kinh doanh (chiến dịch một sản phẩm; giá trị và chi phí liên hệ là GIẢ ĐỊNH, {dom.currency})", ""]
    for c, rows in r["campaign"].items():
        p = dom.product(c)
        L += [f"**{p.label_vi}** (giá trị {p.value:,.0f}, chi phí liên hệ {dom.contact_cost:,.0f}):", "",
              table(rows, ["top", "contacts", "conversions", "random_conversions", "lift", "roi", "roi_random",
                           "expected_conversions_model"])]
    o = r["off_policy"]
    L += ["## 8. Đánh giá ngoại tuyến chính sách (off-policy)", "",
          f"Log mô phỏng: chính sách cũ = sản phẩm phổ biến nhất, thăm dò ε = {o['eps']}; {o['n']:,} khách; giá trị chính "
          f"sách cũ {o['logging_value']:.4f}. Giá trị THẬT của chính sách mới (top-1 v2) = {o['truth']:.4f}.", "",
          "| Bộ ước lượng | Ước lượng | Độ chệch |", "|---|---:|---:|"]
    L += [f"| {k.upper()} | {v:.4f} | {o['bias'][k]:+.4f} |" for k, v in o["estimates"].items()]
    L += ["", "## 9. Đặc trưng quan trọng (gain)", "", ", ".join(f"`{k}` {v:.3f}" for k, v in r["importance"].items()), ""]
    return "\n".join(L)


# ---------------------------------------------------------------------------------------------- online (mô phỏng)
def cmd_online(ds: Dataset, m: Ranker, log=print, users_per_day: int = 1500, days: int = 14) -> dict:
    te = ds.split("test")
    te = te[te["month"] == te["month"].max()].reset_index(drop=True)
    Y, E = matrices(ds, te)
    P = m.rank_scores(ds, te)
    base = baselines(ds, te, log=lambda *_: None)
    ctrl_name = "repeat" if "repeat" in base else "cooccurrence"
    O = {"v2": np.argsort(-np.where(E, P, -np.inf), 1), ctrl_name: np.argsort(-np.where(E, base[ctrl_name], -np.inf), 1),
         "popularity": np.argsort(-np.where(E, base["popularity"], -np.inf), 1)}
    pol = {k: (lambda idx, o=o: o[idx]) for k, o in O.items()}
    ids = te[ds.id_col].astype(str).to_numpy()
    res = {"domain": ds.domain.name, "month": str(te["month"].iat[0]), "users": len(te), "control": ctrl_name,
           "response_model": ON.ResponseModel().__dict__}
    log("  A/B 14 ngay ...")
    sim = ON.simulate_ab(Y, E, ids, {"control": pol[ctrl_name], "treatment": pol["v2"]}, days=days,
                         daily_users=min(users_per_day, len(te)))
    res["ab"] = ON.ab_report(sim)
    res["ab_daily"] = [{k: d[k] for k in ("day", "rate_a", "rate_b", "diff", "p_value", "users_a", "users_b")}
                       for d in sim["daily"]]
    pa = res["ab"]["final"]["rate_a"]
    res["sample_size"] = {f"{x:.0%}": ON.sample_size(pa, x) for x in (0.05, 0.1, 0.2, 0.5)} if pa > 0 else {}
    log("  A/A nhin trom ...")
    res["aa"] = ON.aa_peeking(Y, E, ids, pol[ctrl_name], n_sims=100, days=days, daily_users=300)
    log("  interleaving ...")
    res["interleaving"] = {n: ON.simulate_interleaving(Y, E, pol[ctrl_name], pol["v2"], n) for n in (500, 2000, 8000)}
    log("  bandit Thompson ...")
    res["bandit"] = ON.thompson_policies(Y, E, pol, n_users=min(20000, 4 * len(te)))
    out = REPORTS / ds.domain.name
    out.mkdir(parents=True, exist_ok=True)
    (out / "online_sim.json").write_text(json.dumps(res, ensure_ascii=False, indent=1, default=float), encoding="utf-8")
    (out / "online_sim.md").write_text(online_md(res), encoding="utf-8")
    log(f"  bao cao {out / 'online_sim.md'}")
    return res


def online_md(r: dict) -> str:
    ab, f = r["ab"], r["ab"]["final"]
    L = [f"# Đánh giá online MÔ PHỎNG: {r['domain']} ({time.strftime('%Y-%m-%d')})", "",
         f"Khách ảo rút từ tháng {r['month']} ({r['users']:,} khách), trả lời theo nhãn THẬT của tháng sau. Mô hình phản hồi "
         f"(giả định): `{r['response_model']}`. Đây là kiểm chứng PHƯƠNG PHÁP thử nghiệm, không phải kết quả kinh doanh.", "",
         f"## 1. A/B: control = `{r['control']}`, treatment = v2 (14 ngày, đơn vị = khách)", "",
         "| Chỉ số | Control | Treatment |", "|---|---:|---:|",
         f"| Khách | {f['users_a']:,} | {f['users_b']:,} |",
         f"| Tỉ lệ khách nhận lời chào | {f['rate_a']:.4f} | {f['rate_b']:.4f} |",
         f"| CTR (guardrail) | {ab['guardrails'].get('control', {}).get('ctr', 0):.4f} | {ab['guardrails'].get('treatment', {}).get('ctr', 0):.4f} |",
         f"| Tỉ lệ bỏ qua (guardrail) | {ab['guardrails'].get('control', {}).get('dismiss_rate', 0):.4f} | "
         f"{ab['guardrails'].get('treatment', {}).get('dismiss_rate', 0):.4f} |", "",
         f"Chênh lệch {f['diff']:+.4f} (CI 95% [{f['ci'][0]:+.4f}; {f['ci'][1]:+.4f}]), lift {_f(f['lift'], 3)}, "
         f"p = {f['p_value']:.4g}. SRM: chi2 = {ab['srm']['chi2']:.2f}, p = {ab['srm']['p_value']:.3f} "
         f"({'đạt' if ab['srm']['ok'] else 'HỎNG'}). Ngày đầu tiên p < 0,05: {ab['first_significant_day']}.", "",
         table(r["ab_daily"], ["day", "users_a", "users_b", "rate_a", "rate_b", "diff", "p_value"]),
         "**Cỡ mẫu mỗi nhóm** (alpha 0,05, power 0,8, tỉ lệ nền = control): "
         + ", ".join(f"tăng {k}: {v:,} khách" for k, v in r["sample_size"].items()), "",
         "## 2. A/A và hiện tượng nhìn trộm", "",
         f"{r['aa']['sims']} thử nghiệm A/A × {r['aa']['days']} ngày: dừng khi p < 0,05 ở BẤT KỲ ngày nào cho dương tính giả "
         f"{r['aa']['false_positive_peeking']:.0%}; chỉ xem cuối kỳ: {r['aa']['false_positive_fixed_horizon']:.0%} "
         "(danh nghĩa 5%).", "",
         "## 3. Interleaving team-draft (control vs v2 trong CÙNG danh sách)", "",
         table([{"users": n, **v} for n, v in r["interleaving"].items()], ["users", "a", "b", "tie", "b_win_share", "p_value"]),
         "## 4. Bandit Thompson giữa các chính sách", "",
         f"Giá trị thật mỗi chính sách: {', '.join(f'{k} {v:.4f}' for k, v in r['bandit']['true_rate'].items())}. "
         f"Số lần chọn: {r['bandit']['pulls']}. Regret {r['bandit']['regret']:.1f} so với chia đều {r['bandit']['regret_uniform']:.1f}.", "",
         table(r["bandit"]["curve"], ["t", "regret", "share_best"])]
    return "\n".join(L)


# ---------------------------------------------------------------------------------------------- score
def cmd_score(ds: Dataset, m: Ranker, k: int = 3, log=print):
    """Bảng PHỤC VỤ: mỗi khách nhận danh sách của NHÓM thử nghiệm của mình (online.assign theo băm client_id):
    treatment = v2 (giá trị kỳ vọng), control = sản phẩm phổ biến nhất (cách làm khi chưa có mô hình). Kèm bảng điểm
    đầy đủ (mọi sản phẩm đủ điều kiện) cho trang chiến dịch."""
    import pandas as pd

    d = SV.latest(ds)
    ev = ON.EventLog(EVENTS / f"{ds.domain.name}.jsonl").read()
    grp = np.array([ON.assign(c) for c in d[ds.id_col].astype(str)])
    tr = ds.split("train")
    Ytr, Etr = matrices(ds, tr)
    pop = Ytr.sum(0) / np.maximum(Etr.sum(0), 1)
    parts = []
    for v in ("treatment", "control"):
        dv = d[grp == v].reset_index(drop=True)
        rank_by = None if v == "treatment" else np.tile(pop, (len(dv), 1))
        parts.append(SV.recommend(ds, dv, m, k=k, events=ev, variant=v, rank_by=rank_by,
                                  policy_reason="sản phẩm được nhiều khách chọn nhất"))
    out = pd.concat(parts, ignore_index=True)
    path = SV.save_local(out, REPORTS / ds.domain.name / "reco_topk.parquet")
    full = SV.recommend(ds, d, m, k=len(ds.domain.codes), events=(), variant="all", explain=False)
    SV.save_local(full[["domain", "client_id", "scored_month", "product", "product_label", "p_calibrated",
                        "expected_value", "model_version"]], REPORTS / ds.domain.name / "reco_score.parquet")
    log(f"  {out.client_id.nunique():,} khach, {len(out):,} goi y ({out.variant.value_counts().to_dict()}) -> {path}; "
        f"bang diem {len(full):,} dong; loc treatment: {parts[0].attrs['filtered']}")
    return out


def main(argv=None) -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["train", "evaluate", "online", "score", "publish", "all"])
    ap.add_argument("--domain", choices=["mbd", "berka"], required=True)
    ap.add_argument("--version", default=None)
    ap.add_argument("--k", type=int, default=3)
    a = ap.parse_args(argv)
    t0 = time.time()
    if a.cmd == "publish":
        import pandas as pd

        for name in ("reco_topk", "reco_score"):
            SV.publish(pd.read_parquet(REPORTS / a.domain / f"{name}.parquet"), table=f"{name}_{a.domain}")
        return 0
    ds = load(a.domain)
    print(f"{a.domain}: {len(ds.df):,} dong, {ds.df[ds.id_col].nunique():,} khach [{time.time() - t0:.0f}s]")
    m = cmd_train(ds) if a.cmd in ("train", "all") else Ranker.load(REGISTRY, a.domain, a.version)
    if a.cmd in ("evaluate", "all"):
        r = cmd_evaluate(ds, m)
        card = REGISTRY / a.domain / m.meta["version"] / "model_card.json"
        c = json.loads(card.read_text(encoding="utf-8"))
        c["metrics"] = {"test_ranking": next(x for x in r["ranking"] if "champion" in x["model"]),
                        "calibration_ece_after": {k: v["after"]["ece"] for k, v in r["calibration"].items()},
                        "psi": r["psi"]}
        card.write_text(json.dumps(c, ensure_ascii=False, indent=1, default=float), encoding="utf-8")
    if a.cmd in ("online", "all") and a.domain == "mbd" or a.cmd == "online":
        cmd_online(ds, m)
    if a.cmd in ("score", "all") and a.domain == "berka" or a.cmd == "score":
        cmd_score(ds, m, a.k)
    print(f"xong [{time.time() - t0:.0f}s]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
