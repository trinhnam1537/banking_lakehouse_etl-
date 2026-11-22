"""Đánh giá ONLINE của hệ gợi ý v2 (docs/RECSYS-V2.md mục 6): nhật ký sự kiện, thử nghiệm A/B, interleaving, bandit.

Đồ án chưa có người dùng thật ở quy mô đủ để thử nghiệm, nên có HAI phần tách bạch:

1. Hạ tầng online THẬT (dùng ngay trên portal): sự kiện `RecoEvent` (impression / click / accept / dismiss) ghi JSONL
   (`EventLog`), gán nhóm thử nghiệm ổn định theo băm client_id (`assign`), và `online_metrics` tính CTR, tỉ lệ nhận,
   tỉ lệ bỏ qua theo nhóm + sản phẩm từ log. Đây là thứ sẽ chạy khi có người dùng.

2. Mô phỏng để kiểm chứng PHƯƠNG PHÁP (không phải kết quả kinh doanh): khách ảo trả lời theo nhãn THẬT của tập test
   (khách có mua sản phẩm k trong tháng sau thì dễ nhận lời chào k), có thiên lệch vị trí và mệt mỏi khi bị chào lặp.
   Trên đó chạy: A/B (z-test hai tỉ lệ, kiểm SRM, cỡ mẫu / power, hiện tượng "nhìn trộm" làm tăng dương tính giả),
   interleaving team-draft (nhạy hơn A/B), và bandit Thompson (học online, đo regret).
"""

from __future__ import annotations

import hashlib
import json
import math
import pathlib
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Callable, Dict, Iterable, List, Optional

import numpy as np

EVENT_TYPES = ("impression", "click", "accept", "dismiss")


# ============================================================================== 1. hạ tầng online thật
@dataclass
class RecoEvent:
    client_id: str
    product: str
    event_type: str                         # impression | click | accept | dismiss
    variant: str                            # nhóm thử nghiệm (control / treatment)
    model_version: str
    rank: int = 1
    p_shown: Optional[float] = None         # xác suất hiển thị cho khách (đã hiệu chỉnh)
    surface: str = "portal"                 # portal_rm | portal_customer | agent | campaign
    domain: str = "berka"
    actor: str = ""                         # ai thao tác: RM, khách (persona)
    ts: str = field(default_factory=lambda: time.strftime("%Y-%m-%dT%H:%M:%S"))
    event_id: str = field(default_factory=lambda: uuid.uuid4().hex)

    def __post_init__(self):
        if self.event_type not in EVENT_TYPES:
            raise ValueError(f"event_type {self.event_type!r} khong hop le; chon {EVENT_TYPES}")


class EventLog:
    """Nhật ký sự kiện dạng JSONL (append-only). Một file mỗi miền; đọc lại cho online_metrics và chính sách liên hệ
    (không chào lại sản phẩm khách vừa bỏ qua). Có thể đẩy lên lake (track_a_gold.reco_event) bằng serve.publish_events."""

    def __init__(self, path: pathlib.Path):
        self.path = pathlib.Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, ev: RecoEvent) -> None:
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(ev), ensure_ascii=False) + "\n")

    def read(self) -> List[Dict]:
        if not self.path.exists():
            return []
        return [json.loads(x) for x in self.path.read_text(encoding="utf-8").splitlines() if x.strip()]


def assign(client_id, experiment: str = "reco_v2", treatment_share: float = 0.5) -> str:
    """Gán nhóm ỔN ĐỊNH theo băm (cùng khách luôn cùng nhóm, không cần lưu trạng thái), độc lập giữa các thử nghiệm."""
    h = int(hashlib.sha256(f"{experiment}:{client_id}".encode()).hexdigest()[:8], 16) / 0xFFFFFFFF
    return "treatment" if h < treatment_share else "control"


def online_metrics(events: Iterable[Dict]) -> Dict:
    """CTR = click/impression; tỉ lệ nhận = accept/impression; tỉ lệ bỏ qua = dismiss/impression. Theo nhóm và sản phẩm.
    Đơn vị phân tích là KHÁCH (nhiều lần hiển thị cho một khách không độc lập): đếm khách có ít nhất một accept."""
    agg: Dict[tuple, Dict[str, int]] = {}
    users: Dict[str, Dict[str, set]] = {}
    for e in events:
        for key in ((e["variant"], "*"), (e["variant"], e["product"])):
            agg.setdefault(key, dict.fromkeys(EVENT_TYPES, 0))[e["event_type"]] += 1
        u = users.setdefault(e["variant"], {t: set() for t in EVENT_TYPES})
        u[e["event_type"]].add(e["client_id"])
    rows = []
    for (v, p), c in sorted(agg.items()):
        imp = max(c["impression"], 1)
        rows.append({"variant": v, "product": p, **c, "ctr": c["click"] / imp, "accept_rate": c["accept"] / imp,
                     "dismiss_rate": c["dismiss"] / imp})
    per_user = {v: {"users": len(u["impression"]), "users_accept": len(u["accept"]),
                    "user_accept_rate": len(u["accept"]) / max(len(u["impression"]), 1)} for v, u in users.items()}
    return {"rows": rows, "per_user": per_user}


# ============================================================================== 2. thống kê thử nghiệm
def _phi(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def _z(p: float) -> float:
    """Phân vị chuẩn (xấp xỉ Acklam), đủ chính xác cho tính cỡ mẫu."""
    a = [-39.69683028665376, 220.9460984245205, -275.9285104469687, 138.357751867269, -30.66479806614716, 2.506628277459239]
    b = [-54.47609879822406, 161.5858368580409, -155.6989798598866, 66.80131188771972, -13.28068155288572]
    c = [-0.007784894002430293, -0.3223964580411365, -2.400758277161838, -2.549732539343734, 4.374664141464968, 2.938163982698783]
    d = [0.007784695709041462, 0.3224671290700398, 2.445134137142996, 3.754408661907416]
    q = min(max(p, 1e-12), 1 - 1e-12)
    if q < 0.02425:
        t = math.sqrt(-2 * math.log(q))
        return (((((c[0] * t + c[1]) * t + c[2]) * t + c[3]) * t + c[4]) * t + c[5]) / ((((d[0] * t + d[1]) * t + d[2]) * t + d[3]) * t + 1)
    if q > 1 - 0.02425:
        return -_z(1 - q)
    t = q - 0.5
    r = t * t
    return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * t / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)


def two_proportion(xa: int, na: int, xb: int, nb: int, alpha: float = 0.05) -> Dict:
    """z-test hai tỉ lệ (B so với A) + CI của chênh lệch tuyệt đối + lift tương đối."""
    pa, pb = xa / max(na, 1), xb / max(nb, 1)
    p = (xa + xb) / max(na + nb, 1)
    se0 = math.sqrt(max(p * (1 - p) * (1 / max(na, 1) + 1 / max(nb, 1)), 1e-18))
    z = (pb - pa) / se0
    se = math.sqrt(max(pa * (1 - pa) / max(na, 1) + pb * (1 - pb) / max(nb, 1), 1e-18))
    zc = _z(1 - alpha / 2)
    return {"rate_a": pa, "rate_b": pb, "diff": pb - pa, "ci": (pb - pa - zc * se, pb - pa + zc * se),
            "lift": (pb / pa - 1) if pa > 0 else None, "z": z, "p_value": 2 * (1 - _phi(abs(z)))}


def sample_size(p_base: float, mde_rel: float, alpha: float = 0.05, power: float = 0.8) -> int:
    """Số khách MỖI NHÓM để phát hiện mức tăng tương đối mde_rel của tỉ lệ nền p_base (hai phía)."""
    p2 = p_base * (1 + mde_rel)
    pbar = (p_base + p2) / 2
    za, zb = _z(1 - alpha / 2), _z(power)
    n = (za * math.sqrt(2 * pbar * (1 - pbar)) + zb * math.sqrt(p_base * (1 - p_base) + p2 * (1 - p2))) ** 2 / (p2 - p_base) ** 2
    return int(math.ceil(n))


def srm(na: int, nb: int, share_b: float = 0.5) -> Dict:
    """Sample Ratio Mismatch: chi-bình phương 1 bậc tự do. p < 0,001 => gán nhóm hỏng, KHÔNG đọc kết quả."""
    n = na + nb
    ea, eb = n * (1 - share_b), n * share_b
    chi = (na - ea) ** 2 / ea + (nb - eb) ** 2 / eb
    p = 2 * (1 - _phi(math.sqrt(chi)))
    return {"n_a": na, "n_b": nb, "chi2": chi, "p_value": p, "ok": p >= 0.001}


# ============================================================================== 3. mô phỏng phản hồi khách
@dataclass
class ResponseModel:
    """Khách ảo: P(click | hiển thị sản phẩm k ở vị trí r) = examine[r] * (a_buy nếu nhãn thật = 1, ngược lại a_no).
    P(accept | click) = c_buy / c_no. Mệt mỏi: mỗi lần đã bị chào cùng sản phẩm trước đó nhân hệ số fatigue.
    Bỏ qua (dismiss) với xác suất d_no khi không click. Tham số là GIẢ ĐỊNH, ghi trong báo cáo."""
    examine: tuple = (1.0, 0.6, 0.4)
    a_buy: float = 0.5
    a_no: float = 0.03
    c_buy: float = 0.7
    c_no: float = 0.05
    d_no: float = 0.15
    fatigue: float = 0.7

    def respond(self, rng, label: int, rank: int, seen_before: int) -> str:
        att = (self.a_buy if label else self.a_no) * self.examine[min(rank - 1, len(self.examine) - 1)]
        att *= self.fatigue ** seen_before
        if rng.random() < att:
            return "accept" if rng.random() < (self.c_buy if label else self.c_no) else "click"
        return "dismiss" if rng.random() < self.d_no else "impression"


Policy = Callable[[np.ndarray], np.ndarray]       # chỉ số dòng -> ma trận thứ tự sản phẩm (n, K)


def simulate_ab(Y: np.ndarray, E: np.ndarray, ids: np.ndarray, policies: Dict[str, Policy], k: int = 2,
                rm: Optional[ResponseModel] = None, days: int = 14, daily_users: int = 2000, seed: int = 0,
                treatment_share: float = 0.5) -> Dict:
    """Mỗi ngày daily_users khách (rút từ tập test) vào portal; nhóm theo assign(client_id); xem top-k của chính sách
    của nhóm mình. Trả nhật ký sự kiện + kết quả theo ngày (cho phân tích nhìn trộm)."""
    rm = rm or ResponseModel()
    rng = np.random.default_rng(seed)
    seen: Dict[tuple, int] = {}
    events, daily = [], []
    cum = {v: {"users": set(), "acc_users": set()} for v in ("control", "treatment")}
    for day in range(days):
        rows = rng.choice(len(Y), daily_users, replace=False)
        for i in rows:
            v = assign(ids[i], treatment_share=treatment_share)
            order = policies[v](np.array([i]))[0]
            cum[v]["users"].add(ids[i])
            for r, j in enumerate([j for j in order if E[i, j]][:k], 1):
                key = (ids[i], j)
                out = rm.respond(rng, int(Y[i, j]), r, seen.get(key, 0))
                seen[key] = seen.get(key, 0) + 1
                events.append({"day": day, "client_id": str(ids[i]), "variant": v, "product": int(j), "rank": r,
                               "event_type": "impression"})
                if out != "impression":
                    if out == "accept":
                        events.append({**events[-1], "event_type": "click"})
                        cum[v]["acc_users"].add(ids[i])
                    events.append({**events[-1], "event_type": out})
        ua, ub = len(cum["control"]["users"]), len(cum["treatment"]["users"])
        xa, xb = len(cum["control"]["acc_users"]), len(cum["treatment"]["acc_users"])
        daily.append({"day": day + 1, **two_proportion(xa, ua, xb, ub), "users_a": ua, "users_b": ub})
    return {"events": events, "daily": daily}


def ab_report(sim: Dict, treatment_share: float = 0.5) -> Dict:
    ev = sim["events"]
    m = online_metrics([{**e, "product": str(e["product"])} for e in ev])
    last = sim["daily"][-1]
    pu = m["per_user"]
    g = {v: {"dismiss_rate": next(r["dismiss_rate"] for r in m["rows"] if r["variant"] == v and r["product"] == "*"),
             "ctr": next(r["ctr"] for r in m["rows"] if r["variant"] == v and r["product"] == "*")} for v in pu}
    return {"final": last, "per_user": pu, "guardrails": g,
            "srm": srm(pu.get("control", {}).get("users", 0), pu.get("treatment", {}).get("users", 0), treatment_share),
            "first_significant_day": next((d["day"] for d in sim["daily"] if d["p_value"] < 0.05), None)}


def aa_peeking(Y, E, ids, policy: Policy, n_sims: int = 200, days: int = 14, daily_users: int = 400, seed: int = 0) -> Dict:
    """Thử nghiệm A/A (hai nhóm CÙNG chính sách): tỉ lệ "phát hiện khác biệt" khi xem p-value MỖI NGÀY và dừng ngay khi
    p < 0,05 (nhìn trộm) so với chỉ xem một lần cuối kỳ. Nhìn trộm làm dương tính giả vượt xa 5%."""
    rng = np.random.default_rng(seed)
    rm = ResponseModel()
    fp_peek = fp_fixed = 0
    for s in range(n_sims):
        acc = {0: [0, 0], 1: [0, 0]}
        stopped = False
        for day in range(days):
            for i in rng.choice(len(Y), daily_users, replace=False):
                g = int(rng.random() < 0.5)
                j = next((j for j in policy(np.array([i]))[0] if E[i, j]), None)
                acc[g][1] += 1
                if j is not None and rm.respond(rng, int(Y[i, j]), 1, 0) == "accept":
                    acc[g][0] += 1
            t = two_proportion(acc[0][0], acc[0][1], acc[1][0], acc[1][1])
            if t["p_value"] < 0.05 and not stopped:
                fp_peek += 1
                stopped = True
        fp_fixed += t["p_value"] < 0.05
    return {"sims": n_sims, "days": days, "false_positive_peeking": fp_peek / n_sims,
            "false_positive_fixed_horizon": fp_fixed / n_sims}


# ============================================================================== 4. interleaving
def team_draft(a: List[int], b: List[int], k: int, rng) -> tuple:
    """Team-draft interleaving: hai đội lần lượt chọn (ai đi trước tung đồng xu) món tốt nhất chưa có trong danh sách."""
    out, team, ia, ib = [], [], 0, 0
    na = nb = 0
    while len(out) < k and (ia < len(a) or ib < len(b)):
        pick_a = na < nb or (na == nb and rng.random() < 0.5)
        src, idx = (a, ia) if pick_a else (b, ib)
        while idx < len(src) and src[idx] in out:
            idx += 1
        if idx >= len(src):
            pick_a = not pick_a
            src, idx = (a, ia) if pick_a else (b, ib)
            while idx < len(src) and src[idx] in out:
                idx += 1
            if idx >= len(src):
                break
        out.append(src[idx])
        team.append("a" if pick_a else "b")
        if pick_a:
            ia, na = idx + 1, na + 1
        else:
            ib, nb = idx + 1, nb + 1
    return out, team


def simulate_interleaving(Y, E, pol_a: Policy, pol_b: Policy, n_users: int, k: int = 2, seed: int = 0) -> Dict:
    """Mỗi khách thấy MỘT danh sách trộn từ A và B; click được tính cho đội đã đưa món đó vào. B thắng phiên nếu nhiều
    click hơn. Kiểm định dấu (sign test) trên các phiên có người thắng."""
    rng = np.random.default_rng(seed)
    rm = ResponseModel()
    wins = {"a": 0, "b": 0, "tie": 0}
    for i in rng.choice(len(Y), n_users, replace=len(Y) < n_users):
        la = [j for j in pol_a(np.array([i]))[0] if E[i, j]]
        lb = [j for j in pol_b(np.array([i]))[0] if E[i, j]]
        lst, team = team_draft(la, lb, k, rng)
        cl = {"a": 0, "b": 0}
        for r, (j, t) in enumerate(zip(lst, team), 1):
            if rm.respond(rng, int(Y[i, j]), r, 0) in ("click", "accept"):
                cl[t] += 1
        wins["a" if cl["a"] > cl["b"] else "b" if cl["b"] > cl["a"] else "tie"] += 1
    n = wins["a"] + wins["b"]
    z = (wins["b"] - n / 2) / math.sqrt(n / 4) if n else 0.0
    return {"users": n_users, **wins, "b_win_share": wins["b"] / n if n else None, "z": z, "p_value": 2 * (1 - _phi(abs(z)))}


# ============================================================================== 5. bandit Thompson
def thompson_policies(Y, E, policies: Dict[str, Policy], n_users: int, seed: int = 0) -> Dict:
    """Bandit Beta-Bernoulli giữa các CHÍNH SÁCH gợi ý (học online chính sách nào tốt nhất, thay vì chia cố định 50/50).
    Regret = phần thưởng mất so với luôn chọn chính sách tốt nhất (biết sau khi mô phỏng)."""
    rng = np.random.default_rng(seed)
    rm = ResponseModel()
    names = list(policies)
    a = np.ones(len(names))
    b = np.ones(len(names))
    pulls = np.zeros(len(names), int)
    reward = 0
    users = rng.choice(len(Y), n_users, replace=len(Y) < n_users)
    true_rate = {}
    for nm in names:           # giá trị thật của từng chính sách (top-1, khách mới, không mệt mỏi)
        o = policies[nm](users)
        top = np.array([next((j for j in o[t] if E[u, j]), 0) for t, u in enumerate(users)])
        true_rate[nm] = float(np.mean(Y[users, top] * rm.a_buy * rm.c_buy + (1 - Y[users, top]) * rm.a_no * rm.c_no))
    curve = []
    for t, u in enumerate(users):
        arm = int(np.argmax(rng.beta(a, b)))
        j = next((j for j in policies[names[arm]](np.array([u]))[0] if E[u, j]), None)
        r = int(j is not None and rm.respond(rng, int(Y[u, j]), 1, 0) == "accept")
        a[arm] += r
        b[arm] += 1 - r
        pulls[arm] += 1
        reward += r
        if (t + 1) % max(1, n_users // 20) == 0:
            curve.append({"t": t + 1, "regret": max(true_rate.values()) * (t + 1) - reward,
                          "share_best": float(pulls[names.index(max(true_rate, key=true_rate.get))] / (t + 1))})
    return {"users": n_users, "pulls": dict(zip(names, pulls.tolist())), "true_rate": true_rate,
            "reward": reward, "regret": max(true_rate.values()) * n_users - reward,
            "regret_uniform": max(true_rate.values()) * n_users - np.mean(list(true_rate.values())) * n_users,
            "curve": curve}
