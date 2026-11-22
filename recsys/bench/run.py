"""Benchmark mô hình gợi ý MBD-mini: cùng split, cùng metric với recsys.mbd để so trực tiếp.

    python -m recsys.bench.run [--quick] [--skip gru,cat]

Đọc bảng đặc trưng gốc từ cache của recsys (recsys/.cache, sinh bởi `python -m recsys.mbd`), thêm đặc trưng v2,
huấn luyện từng họ mô hình, trộn (ensemble) chọn trên VALID, báo cáo trên TEST, kèm khoảng tin cậy bootstrap
95% cho chênh lệch Hit@1 / MAP@3 so với LightGBM gốc. Ghi recsys/bench/reports/bench.md + bench.json.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import time

import lightgbm  # noqa: F401  (thứ tự import, xem recsys/__init__.py)
import numpy as np
import pandas as pd

from recsys import mbd
from recsys import metrics as M
from recsys.data import athena_df

from . import models as MD
from .features import FEATURES_V2, LABELS, add_v2, check_no_leak

OUT = pathlib.Path(__file__).resolve().parent / "reports"


def load() -> pd.DataFrame:
    df = athena_df(mbd.FEATURE_SQL)            # dùng cache nếu có, không tốn Athena
    df = add_v2(df)                             # đã sắp theo (client_id, report_month), reset index
    return df


def evaluate(name, Y, S, ks=(0.01, 0.05)):
    pp = [{"model": name, "product": p, **M.per_product(Y[:, j], S[:, j], ks)} for j, p in enumerate(mbd.PRODUCTS)]
    rk = {"model": name, **M.ranking(Y, S, None, k=3)}
    return pp, rk


def bootstrap_diff(Y, S_a, S_b, n=1000, seed=0):
    """CI 95% của (A - B) cho hit@1 và map@3, lấy mẫu lại các dòng test có >= 1 sản phẩm dương (bootstrap ghép cặp)."""
    rows = np.flatnonzero(Y.sum(1) > 0)
    rng = np.random.default_rng(seed)
    out = {"hit@1": [], "map@3": []}
    for _ in range(n):
        r = rows[rng.integers(0, len(rows), len(rows))]
        a, b = M.ranking(Y[r], S_a[r], None, 3), M.ranking(Y[r], S_b[r], None, 3)
        out["hit@1"].append(a["hit@1"] - b["hit@1"])
        out["map@3"].append(a["map@3"] - b["map@3"])
    return {k: (float(np.mean(v)), float(np.percentile(v, 2.5)), float(np.percentile(v, 97.5))) for k, v in out.items()}


def main(argv=None) -> int:
    import sys

    sys.stdout.reconfigure(encoding="utf-8")   # console Windows cp1252 khong in duoc tieng Viet (11-19)
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true", help="it trial tune, it epoch GRU (thu nhanh)")
    ap.add_argument("--skip", default="", help="vd: gru,cat")
    a = ap.parse_args(argv)
    skip = set(filter(None, a.skip.split(",")))
    OUT.mkdir(parents=True, exist_ok=True)
    logf = open(OUT / "bench.log", "w", encoding="utf-8")

    def log(msg):
        print(msg, flush=True)
        logf.write(msg + "\n")
        logf.flush()

    t0 = time.time()
    df = load()
    log(f"du lieu: {len(df):,} dong, {df.client_id.nunique():,} khach, {len(FEATURES_V2)} dac trung v2 "
        f"({len(mbd.FEATURES)} goc) [{time.time() - t0:.0f}s]")
    check_no_leak(df)
    log("kiem tra ro ri: dat (lag1 khop nhan dong truoc, khong dac trung nao |r| >= 0.5 voi nhan)")
    tr, va, te = (df[df.split == s] for s in ("train", "valid", "test"))
    Yv, Yt = va[LABELS].to_numpy(int), te[LABELS].to_numpy(int)
    log(f"train {len(tr):,} / valid {len(va):,} / test {len(te):,}; ty le duong train: "
        + ", ".join(f"{p} {tr[y].mean():.3%}" for p, y in zip(mbd.PRODUCTS, LABELS)))

    res = {"valid": {}, "test": {}, "info": {}, "time_s": {}}

    def add(name, Sv, St, info=None, t=None):
        res["valid"][name], res["test"][name] = Sv, St
        res["info"][name] = info or {}
        res["time_s"][name] = round(t or 0, 1)
        vr = M.ranking(Yv, Sv, None, 3)
        log(f"== {name}: valid map@3={vr['map@3']:.4f} hit@1={vr['hit@1']:.4f} ({t or 0:.0f}s)")

    # --- đối chứng
    pop = tr[LABELS].mean().to_numpy()
    add("popularity", np.tile(pop, (len(va), 1)), np.tile(pop, (len(te), 1)))
    add("repeat", mbd.repeat_score(va) + pop * 1e-3, mbd.repeat_score(te) + pop * 1e-3)

    def run(name, fn):
        if name in skip:
            log(f"(bo qua {name})")
            return
        t = time.time()
        out = fn()
        add(name, *out[:2], out[2] if len(out) > 2 else None, time.time() - t)

    run("lgb_base", lambda: MD.lgb_per_product(tr, va, te, list(mbd.FEATURES), log=log, tag="lgb_base"))
    run("lgb_v2", lambda: MD.lgb_per_product(tr, va, te, FEATURES_V2, log=log, tag="lgb_v2"))
    if "lgb_tuned" not in skip:
        t = time.time()
        best, best_ap, hist = MD.lgb_tune(tr, va, FEATURES_V2, n_trials=4 if a.quick else 16, log=log)
        res["tuning"] = {"best": best, "valid_map@3": best_ap, "trials": hist}
        log(f"tham so tot nhat: {best} (valid map@3={best_ap:.4f})")
        Sv, St, info = MD.lgb_per_product(tr, va, te, FEATURES_V2, params=best, log=log, tag="lgb_tuned")
        add("lgb_tuned", Sv, St, info, time.time() - t)
    run("xgboost", lambda: MD.xgb_per_product(tr, va, te, FEATURES_V2, log=log))
    run("catboost", lambda: MD.cat_per_product(tr, va, te, FEATURES_V2, log=log))
    run("lgb_multitask", lambda: MD.lgb_multitask(tr, va, te, FEATURES_V2, log=log))
    if "gru" not in skip:
        t = time.time()
        out, info = MD.gru_sequence(df, df.split, log=log, epochs=3 if a.quick else 15)
        add("gru", out["valid"][1], out["test"][1], info, time.time() - t)

    # --- ensemble: trung bình xác suất đã hiệu chỉnh của tập con mô hình tốt nhất theo VALID map@3
    cands = [m for m in ("lgb_tuned", "lgb_v2", "xgboost", "catboost", "lgb_multitask", "gru") if m in res["valid"]]
    vscore = {m: M.ranking(Yv, res["valid"][m], None, 3)["map@3"] for m in cands}
    ranked = sorted(cands, key=lambda m: -vscore[m])
    best_set, best_v = None, -1
    for k in range(2, len(ranked) + 1):
        Sv = np.mean([res["valid"][m] for m in ranked[:k]], axis=0)
        v = M.ranking(Yv, Sv, None, 3)["map@3"]
        log(f"  ensemble top{k} {ranked[:k]}: valid map@3={v:.4f}")
        if v > best_v:
            best_set, best_v = ranked[:k], v
    if best_set:
        add("ensemble", np.mean([res["valid"][m] for m in best_set], axis=0),
            np.mean([res["test"][m] for m in best_set], axis=0), {"members": best_set})

    # --- báo cáo TEST
    pp_rows, rk_rows, boot = [], [], {}
    for name, St in res["test"].items():
        pp, rk = evaluate(name, Yt, St)
        pp_rows += pp
        rk_rows.append({**rk, "valid_map@3": M.ranking(Yv, res["valid"][name], None, 3)["map@3"],
                        "time_s": res["time_s"][name]})
        if name not in ("lgb_base", "popularity") and "lgb_base" in res["test"]:
            boot[name] = bootstrap_diff(Yt, St, res["test"]["lgb_base"], n=300 if a.quick else 1000)
    mean_pp = (pd.DataFrame(pp_rows).groupby("model")[["roc_auc", "pr_auc", "lift@1%", "recall@5%"]].mean()
               .reset_index().to_dict("records"))

    md = [f"## Benchmark gợi ý MBD-mini ({time.strftime('%Y-%m-%d')})", "",
          f"Train {len(tr):,} / valid {len(va):,} / test {len(te):,} dòng; {len(FEATURES_V2)} đặc trưng v2 "
          f"({len(mbd.FEATURES)} gốc). Mọi điểm đã hiệu chỉnh về thang xác suất gốc.", "",
          "### Xếp hạng sản phẩm cho từng khách (test, dòng có >= 1 sản phẩm được mua)", "",
          M.table(sorted(rk_rows, key=lambda r: -r.get("map@3", 0)),
                  ["model", "rows", "hit@1", "mrr", "map@3", "valid_map@3", "time_s"]),
          "### Trung bình 4 sản phẩm (nhắm khách cho chiến dịch, test)", "",
          M.table(sorted(mean_pp, key=lambda r: -r["pr_auc"]), ["model", "roc_auc", "pr_auc", "lift@1%", "recall@5%"]),
          "### Chênh lệch so với lgb_base (bootstrap ghép cặp 1000 lần, CI 95%)", "",
          "| model | Δhit@1 | CI 95% | Δmap@3 | CI 95% |", "|---|---|---|---|---|"]
    for name, b in sorted(boot.items(), key=lambda kv: -kv[1]["map@3"][0]):
        h, m = b["hit@1"], b["map@3"]
        md.append(f"| {name} | {h[0]:+.4f} | [{h[1]:+.4f}, {h[2]:+.4f}] | {m[0]:+.4f} | [{m[1]:+.4f}, {m[2]:+.4f}] |")
    md += ["", "### Theo từng sản phẩm (test)", "",
           M.table([r for r in pp_rows if r["model"] != "popularity"],
                   ["model", "product", "pos", "roc_auc", "pr_auc", "lift@1%", "recall@5%"])]
    if "tuning" in res:
        md += ["### Tham số LightGBM tốt nhất (chọn trên valid)", "", f"`{res['tuning']['best']}`", ""]
    if "ensemble" in res["info"]:
        md += [f"Ensemble = trung bình xác suất của {res['info']['ensemble']['members']} (chọn theo valid map@3).", ""]
    (OUT / "bench.md").write_text("\n".join(md), encoding="utf-8")
    (OUT / "bench.json").write_text(json.dumps({"ranking": rk_rows, "per_product": pp_rows, "bootstrap": boot,
                                                "tuning": res.get("tuning"), "info": res["info"]},
                                               indent=1, default=float), encoding="utf-8")
    log("\n".join(md))
    log(f"tong thoi gian {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
