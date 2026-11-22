"""Đánh giá OFFLINE của hệ gợi ý v2 (docs/RECSYS-V2.md mục 4). Mỗi hàm trả dict thuần để ghi JSON / Markdown.

Bảy nhóm câu hỏi, mỗi nhóm một họ chỉ số. Mô hình gợi ý "tốt" phải đạt ở MỌI nhóm, không chỉ nhóm 1:

  1. Độ chính xác xếp hạng (cho từng khách): Hit@1, MRR, MAP@3, NDCG@3, Recall@3
  2. Độ chính xác nhắm khách (cho từng sản phẩm): ROC-AUC, PR-AUC, lift@1%, recall@5%
  3. Hiệu chỉnh xác suất (con số hiển thị có đúng nghĩa không): Brier, ECE, bảng độ tin cậy
  4. Ngoài độ chính xác: độ phủ danh mục, phân bố hiển thị (Gini), độ mới (gợi ý sản phẩm khách chưa từng mua),
     cá nhân hoá (hai khách ngẫu nhiên nhận danh sách khác nhau thế nào)
  5. Công bằng / độ bền theo phân khúc: chỉ số theo phân khúc + tỉ số xấu nhất / tốt nhất
  6. Ổn định theo thời gian: chỉ số theo từng tháng test, PSI phân bố điểm train -> test
  7. Giá trị kinh doanh: chiến dịch top x% -> chuyển đổi kỳ vọng, doanh thu, chi phí, ROI, so với chọn ngẫu nhiên
  + Đánh giá ngoại tuyến chính sách (off-policy, IPS/SNIPS/DR) để ước lượng kết quả online từ log cũ (mục 4.8).
  + Bootstrap ghép cặp: khoảng tin cậy 95% của chênh lệch so với đối chứng.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Sequence

import numpy as np

from recsys import metrics as M


# ---------------------------------------------------------------------------------------------- 1. xếp hạng
def ranking(Y: np.ndarray, S: np.ndarray, E: Optional[np.ndarray] = None, k: int = 3) -> Dict[str, float]:
    S = S.astype(float).copy()
    if E is not None:
        S[~E] = -np.inf
        Y = Y * E
    rows = Y.sum(1) > 0
    Y, S = Y[rows], S[rows]
    if not len(Y):
        return {"rows": 0}
    order = np.argsort(-S, axis=1, kind="stable")
    rel = np.take_along_axis(Y, order, axis=1)
    kk = min(k, Y.shape[1])
    first = np.argmax(rel > 0, axis=1)
    prec = np.cumsum(rel[:, :kk], 1) / np.arange(1, kk + 1)
    ap = (prec * rel[:, :kk]).sum(1) / np.minimum(Y.sum(1), kk)
    disc = 1 / np.log2(np.arange(2, kk + 2))
    dcg = (rel[:, :kk] * disc).sum(1)
    ideal = np.array([disc[:min(int(n), kk)].sum() for n in Y.sum(1)])
    return {"rows": int(len(Y)), "hit@1": float(rel[:, 0].mean()), "mrr": float((1 / (first + 1)).mean()),
            f"map@{kk}": float(ap.mean()), f"ndcg@{kk}": float((dcg / ideal).mean()),
            f"recall@{kk}": float((rel[:, :kk].sum(1) / Y.sum(1)).mean())}


def random_ranking(Y: np.ndarray, E: np.ndarray, k: int = 3, draws: int = 50, seed: int = 0) -> Dict[str, float]:
    """Đối chứng NGẪU NHIÊN trong các sản phẩm đủ điều kiện: trung bình `draws` lần xáo thứ tự. Đây là mức sàn thật
    của bài toán: khi mỗi khách chỉ còn 1-3 sản phẩm đủ điều kiện, đoán bừa cũng đạt Hit@1 cao, nên mọi con số xếp
    hạng phải đọc so với mức này chứ không phải so với 0."""
    rng = np.random.default_rng(seed)
    runs = [ranking(Y, rng.random(Y.shape), E, k) for _ in range(draws)]
    return {key: (float(np.mean([r[key] for r in runs])) if key != "rows" else runs[0]["rows"]) for key in runs[0]}


def eligibility(Y: np.ndarray, E: np.ndarray) -> Dict:
    """Độ khó của bài toán xếp hạng: trên các dòng được chấm (có ≥ 1 sản phẩm đủ điều kiện được mua), mỗi dòng có bao
    nhiêu sản phẩm đủ điều kiện. Dòng chỉ có 1 sản phẩm đủ điều kiện thì mọi mô hình đều đúng (Hit@1 = 1)."""
    rows = (Y * E).sum(1) > 0
    n = E[rows].sum(1)
    pos = (Y * E)[rows].sum(1)
    dist = {int(v): int((n == v).sum()) for v in sorted(set(n.tolist()))}
    return {"rows": int(rows.sum()), "mean_eligible": float(n.mean()) if len(n) else 0.0,
            "mean_positive": float(pos.mean()) if len(pos) else 0.0, "eligible_dist": dist,
            "trivial_share": float((n == 1).mean()) if len(n) else 0.0,
            "random_hit1_expected": float((pos / n).mean()) if len(n) else 0.0}


def nontrivial(E: np.ndarray, min_eligible: int = 2) -> np.ndarray:
    """Mặt nạ các dòng có ít nhất `min_eligible` sản phẩm đủ điều kiện (bài toán xếp hạng thật sự)."""
    return E.sum(1) >= min_eligible


# ---------------------------------------------------------------------------------------------- 2. theo sản phẩm
def per_product(Y, S, E, codes) -> List[Dict]:
    out = []
    for j, c in enumerate(codes):
        m = E[:, j]
        out.append({"product": c, **M.per_product(Y[m, j], S[m, j])})
    return out


# ---------------------------------------------------------------------------------------------- 3. hiệu chỉnh
def calibration(y: np.ndarray, p: np.ndarray, bins: int = 10) -> Dict:
    """Brier = MSE(p, y). ECE = Σ |tỉ lệ thật − p trung bình| theo bin, trọng số = tỉ lệ mẫu. Bin theo PHÂN VỊ của p
    (nhãn hiếm: bin đều theo giá trị sẽ dồn gần hết mẫu vào bin đầu)."""
    y, p = np.asarray(y, float), np.asarray(p, float)
    edges = np.unique(np.quantile(p, np.linspace(0, 1, bins + 1)))
    idx = np.clip(np.searchsorted(edges, p, side="right") - 1, 0, len(edges) - 2)
    tab, ece, mce = [], 0.0, 0.0
    for b in range(len(edges) - 1):
        s = idx == b
        if not s.any():
            continue
        gap = abs(y[s].mean() - p[s].mean())
        ece += s.mean() * gap
        mce = max(mce, gap)
        tab.append({"bin": b, "n": int(s.sum()), "p_mean": float(p[s].mean()), "rate": float(y[s].mean())})
    base = y.mean()
    return {"brier": float(np.mean((p - y) ** 2)), "brier_ref": float(np.mean((base - y) ** 2)),
            "ece": float(ece), "mce": float(mce), "mean_p": float(p.mean()), "rate": float(base), "table": tab}


# ---------------------------------------------------------------------------------------------- 4. ngoài độ chính xác
def beyond_accuracy(S: np.ndarray, E: np.ndarray, ever: Optional[np.ndarray] = None, k: int = 1,
                    seed: int = 0) -> Dict:
    S = np.where(E, S, -np.inf)
    top = np.argsort(-S, axis=1, kind="stable")[:, :k]
    valid = np.isfinite(np.take_along_axis(S, top, 1))
    K = S.shape[1]
    counts = np.bincount(top[valid], minlength=K).astype(float)
    share = counts / max(counts.sum(), 1)
    srt = np.sort(share)
    gini = float(1 - 2 * np.sum(np.cumsum(srt) / max(srt.sum(), 1e-12)) / K + 1 / K) if K > 1 else 0.0
    out = {"coverage": float((counts > 0).mean()), "exposure_share": [float(x) for x in share], "gini": gini}
    if ever is not None:   # độ mới: tỉ lệ gợi ý top-1 là sản phẩm khách CHƯA từng mua
        e1 = np.take_along_axis(ever, top[:, :1], 1)[:, 0]
        out["novelty_top1"] = float((e1 == 0).mean())
    rng = np.random.default_rng(seed)       # cá nhân hoá: 1 - độ trùng top-k giữa hai khách ngẫu nhiên
    a, b = rng.integers(0, len(S), 5000), rng.integers(0, len(S), 5000)
    overlap = [len(set(top[i]) & set(top[j])) / k for i, j in zip(a, b)]
    out["personalization"] = float(1 - np.mean(overlap))
    return out


# ---------------------------------------------------------------------------------------------- 5. phân khúc
def by_segment(seg: np.ndarray, Y, S, E, min_rows: int = 30) -> Dict:
    rows = []
    for g in sorted(set(seg)):
        m = seg == g
        r = ranking(Y[m], S[m], E[m])
        if r.get("rows", 0) >= min_rows:
            rows.append({"segment": str(g), **r})
    vals = [r["map@3"] for r in rows if "map@3" in r]
    return {"rows": rows, "worst_best_ratio": float(min(vals) / max(vals)) if vals and max(vals) > 0 else None}


# ---------------------------------------------------------------------------------------------- 6. ổn định
def psi(expected: np.ndarray, actual: np.ndarray, bins: int = 10) -> float:
    """Population Stability Index: < 0,1 ổn định, 0,1-0,25 cần theo dõi, > 0,25 trôi đáng kể."""
    edges = np.unique(np.quantile(expected, np.linspace(0, 1, bins + 1)))
    if len(edges) < 3:
        return 0.0
    e = np.histogram(np.clip(expected, edges[0], edges[-1]), edges)[0] / len(expected)
    a = np.histogram(np.clip(actual, edges[0], edges[-1]), edges)[0] / len(actual)
    e, a = np.clip(e, 1e-6, None), np.clip(a, 1e-6, None)
    return float(np.sum((a - e) * np.log(a / e)))


def by_month(months: np.ndarray, Y, S, E) -> List[Dict]:
    return [{"month": str(m), **ranking(Y[months == m], S[months == m], E[months == m])} for m in sorted(set(months))]


# ---------------------------------------------------------------------------------------------- 7. kinh doanh
def campaign(y: np.ndarray, p: np.ndarray, value: float, cost: float, fracs: Sequence[float] = (0.01, 0.05, 0.1),
             accept_given_buy: float = 1.0) -> List[Dict]:
    """Chiến dịch một sản phẩm: liên hệ top x% khách theo p. Chuyển đổi = số khách thật sự mua (nhãn) trong nhóm được
    chọn, nhân accept_given_buy. So với chọn NGẪU NHIÊN cùng số khách (kỳ vọng = n * tỉ lệ nền)."""
    order = np.argsort(-p, kind="stable")
    out = []
    for f in fracs:
        n = max(1, int(round(f * len(y))))
        conv = y[order[:n]].sum() * accept_given_buy
        rnd = n * y.mean() * accept_given_buy
        rev, spend = conv * value, n * cost
        out.append({"top": f"{f:.0%}", "contacts": n, "conversions": float(conv), "random_conversions": float(rnd),
                    "lift": float(conv / rnd) if rnd > 0 else None, "revenue": float(rev), "cost": float(spend),
                    "roi": float((rev - spend) / spend) if spend else None,
                    "roi_random": float((rnd * value - spend) / spend) if spend else None,
                    "expected_conversions_model": float(p[order[:n]].sum())})
    return out


# ---------------------------------------------------------------------------------------------- 4.8 off-policy
def off_policy(Y: np.ndarray, E: np.ndarray, target: np.ndarray, reward_model: np.ndarray, pop: np.ndarray,
               eps: float = 0.3, seed: int = 0) -> Dict:
    """Ước lượng ONLINE từ log của chính sách cũ (đánh giá chính sách ngoại tuyến).

    Mô phỏng log: chính sách cũ μ chọn 1 sản phẩm cho mỗi khách = sản phẩm phổ biến nhất (đủ điều kiện) với xác suất
    1-ε, ngẫu nhiên đều với xác suất ε (thăm dò). Phần thưởng quan sát = nhãn của ĐÚNG sản phẩm đã chọn.
    Chính sách mới π = top-1 của mô hình (`target`, chỉ số sản phẩm). Ước lượng giá trị V(π) = tỉ lệ khách nhận:
      IPS   = mean( 1[a = π(x)] r / μ(a|x) )               không chệch, phương sai lớn
      SNIPS = Σ w r / Σ w                                   tự chuẩn hoá, ít phương sai hơn
      DM    = mean( r̂(x, π(x)) )                           dùng mô hình phần thưởng, chệch nếu mô hình sai
      DR    = DM + mean( w (r − r̂(x, a)) )                 kép: đúng nếu MỘT trong hai (μ hoặc r̂) đúng
    Vì MBD/Berka có nhãn của MỌI sản phẩm, giá trị THẬT V(π) = mean(Y[x, π(x)]) tính được: kiểm được độ chệch của
    từng bộ ước lượng — điều không làm được trên log thật."""
    rng = np.random.default_rng(seed)
    n, K = Y.shape
    elig_n = E.sum(1)
    ok = elig_n > 0
    Y, E, target, R, elig_n = Y[ok], E[ok], target[ok], reward_model[ok], elig_n[ok]
    n = len(Y)
    greedy = np.argmax(np.where(E, pop[None, :], -np.inf), 1)
    mu = np.where(E, eps / elig_n[:, None], 0.0)
    mu[np.arange(n), greedy] += 1 - eps
    a = np.array([rng.choice(K, p=mu[i]) for i in range(n)])
    r = Y[np.arange(n), a].astype(float)
    pa = mu[np.arange(n), a]
    w = (a == target) / pa
    rhat_a = R[np.arange(n), a]
    rhat_pi = R[np.arange(n), target]
    est = {"ips": float(np.mean(w * r)), "snips": float(np.sum(w * r) / max(np.sum(w), 1e-12)),
           "dm": float(np.mean(rhat_pi)), "dr": float(np.mean(rhat_pi + w * (r - rhat_a)))}
    truth = float(np.mean(Y[np.arange(n), target]))
    return {"n": int(n), "eps": eps, "logging_value": float(r.mean()), "truth": truth,
            "estimates": est, "bias": {k: v - truth for k, v in est.items()},
            "match_rate": float(np.mean(a == target))}


# ---------------------------------------------------------------------------------------------- CI
def bootstrap_diff(Y, Sa, Sb, E=None, n: int = 1000, seed: int = 0) -> Dict[str, tuple]:
    rows = np.flatnonzero((Y * (E if E is not None else 1)).sum(1) > 0)
    rng = np.random.default_rng(seed)
    acc = {"hit@1": [], "map@3": [], "ndcg@3": []}
    for _ in range(n):
        r = rows[rng.integers(0, len(rows), len(rows))]
        Ee = E[r] if E is not None else None
        a, b = ranking(Y[r], Sa[r], Ee), ranking(Y[r], Sb[r], Ee)
        for k in acc:
            acc[k].append(a[k] - b[k])
    return {k: (float(np.mean(v)), float(np.percentile(v, 2.5)), float(np.percentile(v, 97.5))) for k, v in acc.items()}
