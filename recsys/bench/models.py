"""Các họ mô hình so sánh cho MBD-mini. Mọi hàm nhận (tr, va, te) đã chia theo thời gian và trả về
(S_valid, S_test): ma trận xác suất (n, 4) ĐÃ HIỆU CHỈNH về thang gốc (bù lấy mẫu âm), để so được giữa
các mô hình và trộn (ensemble) được.

Quy ước huấn luyện chung (giống recsys.trainer để so công bằng):
  - lấy mẫu âm trên TRAIN với tỉ lệ NEG_RATE, giữ mọi dòng dương, seed = SEED + j;
  - dừng sớm theo average precision (PR-AUC) trên VALID, patience 100;
  - TEST chỉ dùng một lần để báo cáo.
"""

from __future__ import annotations

import lightgbm as lgb  # noqa: I001  (phải trước pandas/pyarrow trên Windows, xem recsys/__init__.py)
import numpy as np

from .features import LABELS

NEG_RATE = 0.25
SEED = 7
PATIENCE = 100


def calibrate(p: np.ndarray, neg_rate: float = NEG_RATE) -> np.ndarray:
    """Mô hình học trên dữ liệu chỉ giữ tỉ lệ r của mẫu âm => odds bị nhân 1/r. Hiệu chỉnh: odds_goc = odds * r."""
    p = np.clip(p, 1e-12, 1 - 1e-12)
    o = p / (1 - p) * neg_rate
    return o / (1 + o)


def _X(d, cols):
    return np.ascontiguousarray(d[cols].to_numpy(dtype=np.float32, na_value=np.nan))


def _sample(y, j):
    return (y > 0) | (np.random.default_rng(SEED + j).random(len(y)) < NEG_RATE)


# ------------------------------------------------------------------ LightGBM (mỗi sản phẩm một mô hình)

LGB_BASE = {"objective": "binary", "learning_rate": 0.05, "num_leaves": 31, "min_data_in_leaf": 200,
            "feature_fraction": 0.8, "bagging_fraction": 0.8, "bagging_freq": 1, "lambda_l2": 1.0,
            "metric": "average_precision", "verbosity": -1}


def lgb_per_product(tr, va, te, cols, params=None, log=print, tag="lgb"):
    P = {**LGB_BASE, **(params or {})}
    max_bin = P.pop("max_bin", 255)           # thuộc Dataset, không đổi được sau khi dựng
    Xtr, Xva, Xte = _X(tr, cols), _X(va, cols), _X(te, cols)
    Sv, St, info = np.zeros((len(va), 4)), np.zeros((len(te), 4)), {}
    for j, y in enumerate(LABELS):
        ytr, yva = tr[y].to_numpy(float), va[y].to_numpy(float)
        keep = _sample(ytr, j)
        dtr = lgb.Dataset(Xtr[keep], ytr[keep], feature_name=list(cols), free_raw_data=False,
                          params={"max_bin": max_bin}).construct()
        dva = lgb.Dataset(Xva, yva, reference=dtr, free_raw_data=False).construct()
        m = lgb.train({**P, "seed": SEED, "max_bin": max_bin}, dtr, 3000, valid_sets=[dva],
                      callbacks=[lgb.early_stopping(PATIENCE, verbose=False)])
        Sv[:, j] = calibrate(m.predict(Xva, num_iteration=m.best_iteration))
        St[:, j] = calibrate(m.predict(Xte, num_iteration=m.best_iteration))
        info[y] = {"best_iter": m.best_iteration, "valid_ap": m.best_score["valid_0"]["average_precision"]}
        log(f"  {tag}/{y}: iter={m.best_iteration} valid_ap={info[y]['valid_ap']:.4f}")
    return Sv, St, info


def lgb_tune(tr, va, cols, n_trials=16, log=print):
    """Tìm siêu tham số ngẫu nhiên. Tiêu chí chọn = MAP@3 trên VALID (mục tiêu thật: xếp hạng sản phẩm cho từng
    khách), KHÔNG phải PR-AUC trung bình: thử nghiệm đầu cho thấy tối ưu PR-AUC từng sản phẩm làm MAP@3 giảm
    (điểm giữa các sản phẩm lệch thang). Không chạm tập test."""
    from recsys import metrics as M

    rng = np.random.default_rng(123)
    space = {"num_leaves": [7, 15, 31, 63, 127], "min_data_in_leaf": [50, 100, 200, 500, 1000],
             "learning_rate": [0.02, 0.03, 0.05], "feature_fraction": [0.5, 0.7, 0.9],
             "lambda_l2": [0.0, 1.0, 5.0, 20.0], "min_gain_to_split": [0.0, 0.1, 0.5], "max_bin": [63, 255]}
    Yv = va[LABELS].to_numpy(int)
    best, best_v, hist = None, -1.0, []
    for t in range(n_trials):
        params = {k: (v[rng.integers(len(v))]).item() if hasattr(v[0], "item") else v[rng.integers(len(v))]
                  for k, v in space.items()}
        Sv, _, info = lgb_per_product(tr, va, va.iloc[:1], cols, params=params, log=lambda *_: None, tag="tune")
        v = M.ranking(Yv, Sv, None, 3)["map@3"]
        apm = float(np.mean([i["valid_ap"] for i in info.values()]))
        hist.append({**params, "valid_map@3": v, "valid_ap_mean": apm})
        log(f"  tune {t + 1}/{n_trials}: valid map@3={v:.4f} ap_mean={apm:.4f} {params}")
        if v > best_v:
            best, best_v = params, v
    return best, best_v, hist


# ------------------------------------------------------------------ XGBoost

def xgb_per_product(tr, va, te, cols, log=print):
    import xgboost as xgb

    Xtr, Xva, Xte = _X(tr, cols), _X(va, cols), _X(te, cols)
    Sv, St, info = np.zeros((len(va), 4)), np.zeros((len(te), 4)), {}
    params = {"objective": "binary:logistic", "eval_metric": "aucpr", "tree_method": "hist", "eta": 0.05,
              "max_depth": 6, "min_child_weight": 20, "subsample": 0.8, "colsample_bytree": 0.8,
              "lambda": 1.0, "seed": SEED, "nthread": 8}
    for j, y in enumerate(LABELS):
        ytr, yva = tr[y].to_numpy(float), va[y].to_numpy(float)
        keep = _sample(ytr, j)
        dtr, dva = xgb.DMatrix(Xtr[keep], ytr[keep]), xgb.DMatrix(Xva, yva)
        m = xgb.train(params, dtr, 3000, evals=[(dva, "valid")], early_stopping_rounds=PATIENCE, verbose_eval=False)
        rng = (0, m.best_iteration + 1)
        Sv[:, j] = calibrate(m.predict(dva, iteration_range=rng))
        St[:, j] = calibrate(m.predict(xgb.DMatrix(Xte), iteration_range=rng))
        info[y] = {"best_iter": m.best_iteration, "valid_ap": float(m.best_score)}
        log(f"  xgb/{y}: iter={m.best_iteration} valid_aucpr={m.best_score:.4f}")
    return Sv, St, info


# ------------------------------------------------------------------ CatBoost

def cat_per_product(tr, va, te, cols, log=print):
    from catboost import CatBoostClassifier, Pool

    Xtr, Xva, Xte = _X(tr, cols), _X(va, cols), _X(te, cols)
    Sv, St, info = np.zeros((len(va), 4)), np.zeros((len(te), 4)), {}
    for j, y in enumerate(LABELS):
        ytr, yva = tr[y].to_numpy(float), va[y].to_numpy(float)
        keep = _sample(ytr, j)
        m = CatBoostClassifier(iterations=3000, learning_rate=0.05, depth=6, l2_leaf_reg=3.0,
                               loss_function="Logloss", eval_metric="PRAUC", random_seed=SEED,
                               od_type="Iter", od_wait=PATIENCE, use_best_model=True, thread_count=8, verbose=False,
                               allow_writing_files=False)
        m.fit(Pool(Xtr[keep], ytr[keep]), eval_set=Pool(Xva, yva))
        Sv[:, j] = calibrate(m.predict_proba(Xva)[:, 1])
        St[:, j] = calibrate(m.predict_proba(Xte)[:, 1])
        info[y] = {"best_iter": int(m.get_best_iteration()), "valid_ap": float(m.get_best_score()["validation"]["PRAUC"])}
        log(f"  cat/{y}: iter={info[y]['best_iter']} valid_prauc={info[y]['valid_ap']:.4f}")
    return Sv, St, info


# ------------------------------------------------------------------ LightGBM gộp đa sản phẩm (multi-task "long")

def _long(d, cols):
    """Mỗi dòng (khách, tháng) nhân 4: thêm mã sản phẩm và 'own_*' = lịch sử của CHÍNH sản phẩm đó, để một mô hình
    chung học được quy luật lặp lại dùng chung giữa các sản phẩm (giúp sp2 chỉ có vài trăm ca dương)."""
    import pandas as pd

    parts = []
    for j, k in enumerate(range(1, 5)):
        x = d[cols].copy()
        x["product"] = j
        for s in ("lag1", "lag2", "lag3", "cnt", "since", "rate", "ever", "recent3"):
            x[f"own_{s}"] = d[f"t{k}_{s}"].to_numpy()
        x["y"] = d[f"t{k}"].to_numpy()
        x["_row"] = np.arange(len(d))
        parts.append(x)
    return pd.concat(parts, ignore_index=True)


def lgb_multitask(tr, va, te, cols, log=print):
    own = [f"own_{s}" for s in ("lag1", "lag2", "lag3", "cnt", "since", "rate", "ever", "recent3")]
    fcols = list(cols) + own + ["product"]
    Ltr, Lva, Lte = _long(tr, cols), _long(va, cols), _long(te, cols)
    keep = (Ltr["y"] > 0).to_numpy() | (np.random.default_rng(SEED).random(len(Ltr)) < NEG_RATE)
    pi = [fcols.index("product")]
    dtr = lgb.Dataset(_X(Ltr, fcols)[keep], Ltr["y"].to_numpy(float)[keep], feature_name=fcols,
                      categorical_feature=pi, free_raw_data=False).construct()
    dva = lgb.Dataset(_X(Lva, fcols), Lva["y"].to_numpy(float), feature_name=fcols, categorical_feature=pi,
                      reference=dtr, free_raw_data=False).construct()
    m = lgb.train({**LGB_BASE, "num_leaves": 63, "seed": SEED}, dtr, 3000, valid_sets=[dva],
                  callbacks=[lgb.early_stopping(PATIENCE, verbose=False)])
    log(f"  multitask: iter={m.best_iteration} valid_ap={m.best_score['valid_0']['average_precision']:.4f}")

    def to_wide(L, n):
        S = np.zeros((n, 4))
        S[L["_row"].to_numpy(), L["product"].to_numpy()] = calibrate(m.predict(_X(L, fcols), num_iteration=m.best_iteration))
        return S
    return to_wide(Lva, len(va)), to_wide(Lte, len(te)), {"best_iter": m.best_iteration}


# ------------------------------------------------------------------ GRU theo chuỗi tháng

SEQ_INPUTS = ["trx_m", "dtd", "log_tc", "zero_streak", "moy"]


def gru_sequence(df, splits, log=print, epochs=12, hidden=64, seed=SEED):
    """Mạng hồi quy GRU nhân quả: đầu ra ở bước m chỉ thấy đầu vào bước <= m. Đầu vào bước m gồm hoạt động
    giao dịch tới m và nhãn của bước m-1 (lag1: việc mua trong tháng m, đã biết). Nhãn bước m là việc mua
    tháng m+1 => không rò rỉ. Mất mát BCE có trọng số dương theo sản phẩm (thay cho lấy mẫu âm) => đầu ra đã
    ở thang xác suất gốc khi pos_weight được bù lại (xem calibrate_weighted)."""
    import torch
    from torch import nn

    torch.manual_seed(seed)
    torch.set_num_threads(8)
    d = df.sort_values(["client_id", "report_month"]).reset_index(drop=True)
    lag1 = [f"t{k}_lag1" for k in range(1, 5)]
    feats = SEQ_INPUTS + lag1
    X = d[feats].astype(float).fillna(0.0).to_numpy(np.float32)
    mu, sd = X[d["split"] == "train"].mean(0), X[d["split"] == "train"].std(0) + 1e-6
    sd[-4:], mu[-4:] = 1.0, 0.0                              # giữ nguyên nhãn trễ 0/1
    X = (X - mu) / sd
    Y = d[LABELS].to_numpy(np.float32)
    split = d["split"].to_numpy()
    cid = d["client_id"].to_numpy()
    starts = np.flatnonzero(np.r_[True, cid[1:] != cid[:-1]])
    lens = np.diff(np.r_[starts, len(d)])
    T = lens.max()
    n = len(starts)
    Xs = np.zeros((n, T, X.shape[1]), np.float32)
    Ys = np.zeros((n, T, 4), np.float32)
    Ms = {s: np.zeros((n, T), bool) for s in ("train", "valid", "test")}
    idx = np.full((n, T), -1)
    for i, (s0, L) in enumerate(zip(starts, lens)):
        Xs[i, :L], Ys[i, :L], idx[i, :L] = X[s0:s0 + L], Y[s0:s0 + L], np.arange(s0, s0 + L)
        for s in Ms:
            Ms[s][i, :L] = split[s0:s0 + L] == s
    pos = Y[split == "train"].mean(0)
    pos_w = torch.tensor(np.clip((1 - pos) / pos, 1, 50), dtype=torch.float32)   # cân bằng một phần

    class Net(nn.Module):
        def __init__(self):
            super().__init__()
            self.gru = nn.GRU(X.shape[1], hidden, num_layers=2, batch_first=True, dropout=0.1)
            self.head = nn.Sequential(nn.Linear(hidden, hidden), nn.ReLU(), nn.Linear(hidden, 4))

        def forward(self, x):
            h, _ = self.gru(x)
            return self.head(h)

    net = Net()
    opt = torch.optim.AdamW(net.parameters(), lr=2e-3, weight_decay=1e-4)
    lossf = nn.BCEWithLogitsLoss(pos_weight=pos_w, reduction="none")
    Xt, Yt = torch.from_numpy(Xs), torch.from_numpy(Ys)
    Mtr = torch.from_numpy(Ms["train"])
    from sklearn.metrics import average_precision_score

    def predict():
        net.eval()
        with torch.no_grad():
            out = torch.cat([torch.sigmoid(net(Xt[i:i + 4096])) for i in range(0, n, 4096)]).numpy()
        net.train()
        return out

    best, best_ap, wait, bs = None, -1, 0, 512
    rng = np.random.default_rng(seed)
    for ep in range(epochs):
        perm = rng.permutation(n)
        tot = 0.0
        for b in range(0, n, bs):
            ii = perm[b:b + bs]
            l = lossf(net(Xt[ii]), Yt[ii])
            m = Mtr[ii].unsqueeze(-1).float()
            loss = (l * m).sum() / m.sum().clamp(min=1) / 4
            opt.zero_grad()
            loss.backward()
            nn.utils.clip_grad_norm_(net.parameters(), 1.0)
            opt.step()
            tot += float(loss)
        P = predict()
        mv = Ms["valid"]
        ap = np.mean([average_precision_score(Ys[mv][:, j], P[mv][:, j]) for j in range(4)])
        log(f"  gru epoch {ep + 1}: loss={tot:.1f} valid_ap_mean={ap:.4f}")
        if ap > best_ap:
            best, best_ap, wait = P, ap, 0
        else:
            wait += 1
            if wait >= 3:
                break

    def unweight(p):   # pos_weight w nhân odds dương lên w: odds_goc = odds / w
        p = np.clip(p, 1e-12, 1 - 1e-12)
        o = p / (1 - p) / pos_w.numpy()
        return o / (1 + o)

    out = {}
    for s in ("valid", "test"):
        m = Ms[s]
        S = np.zeros((int((split == s).sum()), 4))
        rows = idx[m]                                   # vị trí trong d
        order = np.argsort(rows)
        S[:] = unweight(best[m])[order]
        out[s] = (np.sort(rows), S)
    return out, {"valid_ap_mean": best_ap}
