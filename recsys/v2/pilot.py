"""Pilot với NGƯỜI DÙNG THẬT cho hệ gợi ý v2 (docs/RECSYS-PILOT.md).

    python -m recsys.v2.pilot build --participants 8      # sinh bài đánh giá -> recsys/v2/pilot/tasks.parquet
    streamlit run apps/portal/app.py                      # người tham gia làm bài ở trang "Đánh giá mù (pilot)"
    python -m recsys.v2.pilot analyze [--sus sus.csv]     # -> recsys/v2/reports/berka/pilot.md

Vì sao cần: mô phỏng online (online.py) cho khách ảo trả lời theo nhãn thật, nên nó chỉ kiểm chứng PHƯƠNG PHÁP
thử nghiệm, không đo được người dùng thật thấy gợi ý thế nào. Pilot đo ba thứ mà offline và mô phỏng không đo được:

  1. Sở thích mù: người đóng vai RM xem hồ sơ khách, chọn danh sách A hay B (một là v2, một là độ phổ biến, thứ tự
     ngẫu nhiên, nhãn ẩn). Đây là interleaving / so sánh cặp do NGƯỜI làm: nhạy hơn A/B nhiều lần với ít người.
  2. Lý do có ích không: chấm 1-5 lời giải thích SHAP của v2, và "lý do có khớp hồ sơ không".
  3. Khả dụng: bảng hỏi SUS (10 câu) sau phiên.

Bài lấy từ tháng TEST (đã biết khách mua gì 3 tháng sau), nên phân tích thêm được: người chọn danh sách nào thì
top-1 của danh sách đó có trúng sản phẩm khách thật sự mua không (người so với mô hình so với hành vi thật).

Chỉ chọn khách mà top-1 của hai chính sách KHÁC nhau (nếu giống nhau thì so sánh vô nghĩa). Mỗi người làm
TASKS_PER_PERSON bài: ANCHORS bài chung cho mọi người (đo độ đồng thuận giữa người chấm) + phần còn lại riêng.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import pathlib
import sys
import time
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional

HERE = pathlib.Path(__file__).resolve().parent
PILOT = HERE / "pilot"
TASKS = PILOT / "tasks.parquet"
RESPONSES = HERE / "events" / "pilot.jsonl"
TASKS_PER_PERSON = 20
ANCHORS = 5
SHOW = 2                     # số gợi ý mỗi danh sách (giống trang "Ưu đãi cho bạn")
PROFILE = [("age", "Tuổi"), ("is_male", "Nam"), ("tenure_months", "Thâm niên (tháng)"),
           ("avg_balance_3m", "Số dư BQ 3 tháng"), ("inflow_3m", "Tiền vào 3 tháng"),
           ("outflow_3m", "Tiền ra 3 tháng"), ("cash_wd_cnt_3m", "Số lần rút tiền mặt 3 tháng"),
           ("overdraft_3m", "Thấu chi 3 tháng"), ("pension_3m", "Lương hưu 3 tháng"), ("region", "Vùng")]


@dataclass
class Response:
    participant: str
    task_id: str
    choice: str                     # "A" | "B" | "same"
    v2_side: str                    # "A" | "B": danh sách nào là v2 (ghi lúc trả lời, người tham gia không thấy)
    reason_useful: Optional[int]    # 1-5: lời giải thích của gợi ý v2 có giúp RM nói chuyện với khách không
    reason_fits: Optional[str]      # "yes" | "partly" | "no": lý do có khớp hồ sơ không
    pitch: Optional[str]            # sản phẩm người tham gia sẽ chào nếu chỉ được chào một
    seconds: float
    comment: str = ""
    ts: str = field(default_factory=lambda: time.strftime("%Y-%m-%dT%H:%M:%S"))


def write_response(r: Response, path: pathlib.Path = RESPONSES) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(asdict(r), ensure_ascii=False) + "\n")


def read_responses(path: pathlib.Path = RESPONSES) -> List[Dict]:
    if not path.exists():
        return []
    return [json.loads(x) for x in path.read_text(encoding="utf-8").splitlines() if x.strip()]


def _h(*parts) -> int:
    return int(hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest()[:12], 16)


def assignment(task_ids: List[str], participant: str, n: int = TASKS_PER_PERSON, anchors: int = ANCHORS) -> List[str]:
    """Bài cho một người: `anchors` bài đầu danh sách (chung) + bài riêng chọn ổn định theo băm (người, bài).
    Thứ tự làm xáo theo người để bài chung không luôn ở đầu (hiệu ứng mệt / học)."""
    common = task_ids[:anchors]
    rest = sorted(task_ids[anchors:], key=lambda t: _h(participant, t))[: n - anchors]
    return sorted(common + rest, key=lambda t: _h("order", participant, t))


def v2_side(participant: str, task_id: str) -> str:
    return "A" if _h("side", participant, task_id) % 2 == 0 else "B"


# ---------------------------------------------------------------------------------------------- build
def build(participants: int = 8, seed: int = 0, log=print):
    import lightgbm  # noqa: F401
    import numpy as np
    import pandas as pd

    from . import serve as SV
    from .data import load, matrices
    from .model import Ranker
    from .run import REGISTRY

    ds = load("berka")
    m = Ranker.load(REGISTRY, "berka")
    te = ds.split("test")
    te = te[te["month"] == te["month"].min()].reset_index(drop=True)        # tháng test đầu: nhãn 3 tháng sau đủ
    Y, E = matrices(ds, te)
    R = m.rank_scores(ds, te)
    P = m.predict(ds, te)
    tr = ds.split("train")
    Ytr, Etr = matrices(ds, tr)
    pop = Ytr.sum(0) / np.maximum(Etr.sum(0), 1)
    codes = ds.domain.codes
    ov2 = np.argsort(-np.where(E, R, -np.inf), 1)
    opop = np.argsort(-np.where(E, np.tile(pop, (len(te), 1)), -np.inf), 1)
    differ = (ov2[:, 0] != opop[:, 0]) & (E.sum(1) >= 2)
    bought = Y.sum(1) > 0
    # nửa bài là khách THẬT SỰ mua (để đo người có nhận ra sản phẩm đúng không), nửa là khách không mua
    rng = np.random.default_rng(seed)
    n_total = ANCHORS + participants * (TASKS_PER_PERSON - ANCHORS)
    pick = []
    for mask, share in ((differ & bought, 0.5), (differ & ~bought, 0.5)):
        idx = np.flatnonzero(mask)
        pick += list(rng.choice(idx, min(len(idx), int(math.ceil(n_total * share))), replace=False))
    pick = rng.permutation(pick)
    sub = te.iloc[pick].reset_index(drop=True)
    contrib = m.contributions(ds, sub, ov2[pick, 0])           # lý do SHAP của gợi ý top-1 v2, như trang RM
    reasons = ["; ".join(SV.reason_text(n_) for n_, _ in c if n_ not in SV.NOT_A_REASON) for c in contrib]
    rows = []
    for n_, i in enumerate(pick):
        def lst(order):
            return [codes[j] for j in order[:SHOW] if E[i, j]]
        prof = {lab: (None if pd.isna(te.at[i, c]) else (te.at[i, c].item() if hasattr(te.at[i, c], "item") else te.at[i, c]))
                for c, lab in PROFILE if c in te}
        prof["Đang giữ"] = ", ".join(ds.domain.product(c).label_vi for c in codes if not E[i, codes.index(c)]) or "chỉ tài khoản"
        rows.append({"task_id": f"T{n_:03d}", "client_id": str(te.at[i, ds.id_col]), "month": str(te.at[i, "month"]),
                     "profile": json.dumps(prof, ensure_ascii=False, default=str),
                     "list_v2": json.dumps(lst(ov2[i])), "list_pop": json.dumps(lst(opop[i])),
                     "p_v2": json.dumps({codes[j]: float(P[i, j]) for j in range(len(codes)) if E[i, j]}),
                     "reason_v2": reasons[n_], "top_v2": codes[ov2[i, 0]],
                     "bought": json.dumps([codes[j] for j in range(len(codes)) if Y[i, j]]),
                     "model_version": m.meta.get("version")})
    out = pd.DataFrame(rows)
    PILOT.mkdir(parents=True, exist_ok=True)
    out.to_parquet(TASKS, index=False)
    log(f"  {len(out)} bai ({int(bought[pick].sum())} khach co mua) cho {participants} nguoi x {TASKS_PER_PERSON} bai "
        f"({ANCHORS} bai chung) -> {TASKS}")
    return out


# ---------------------------------------------------------------------------------------------- phân tích
def _binom_two_sided(k: int, n: int) -> float:
    if n == 0:
        return 1.0
    pk = [math.comb(n, i) * 0.5 ** n for i in range(n + 1)]
    return float(min(1.0, sum(p for p in pk if p <= pk[k] + 1e-12)))


def _wilson(k: int, n: int, z: float = 1.96):
    if n == 0:
        return (0.0, 1.0)
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (c - h, c + h)


def sus_score(answers: List[int]) -> float:
    """SUS chuẩn: 10 câu thang 1-5; câu lẻ (tích cực) = điểm − 1, câu chẵn (tiêu cực) = 5 − điểm; tổng × 2,5."""
    if len(answers) != 10:
        raise ValueError("SUS can dung 10 cau")
    return 2.5 * sum((a - 1) if i % 2 == 0 else (5 - a) for i, a in enumerate(answers))


def analyze(responses: List[Dict], tasks, sus_rows: Optional[List[List[int]]] = None, n_boot: int = 2000,
            seed: int = 0) -> Dict:
    import numpy as np

    T = {r["task_id"]: r for r in tasks.to_dict("records")}
    rs = [r for r in responses if r["task_id"] in T]
    # bỏ trả lời trùng (người bấm hai lần): giữ lần CUỐI cho mỗi (người, bài)
    last = {}
    for r in rs:
        last[(r["participant"], r["task_id"])] = r
    rs = list(last.values())
    win = [r for r in rs if r["choice"] in ("A", "B")]
    k = sum(r["choice"] == r["v2_side"] for r in win)
    n = len(win)
    res = {"responses": len(rs), "participants": len({r["participant"] for r in rs}),
           "preference": {"v2_wins": k, "pop_wins": n - k, "same": len(rs) - n,
                          "v2_win_share": k / n if n else None, "ci95_wilson": _wilson(k, n),
                          "p_sign_test": _binom_two_sided(k, n)}}
    # bootstrap theo NGƯỜI (các câu trả lời của một người không độc lập)
    people = sorted({r["participant"] for r in win})
    if len(people) >= 2:
        rng = np.random.default_rng(seed)
        by = {p: [r["choice"] == r["v2_side"] for r in win if r["participant"] == p] for p in people}
        boots = []
        for _ in range(n_boot):
            s = [x for p in rng.choice(people, len(people)) for x in by[p]]
            boots.append(np.mean(s) if s else np.nan)
        res["preference"]["ci95_cluster_bootstrap"] = (float(np.nanpercentile(boots, 2.5)),
                                                       float(np.nanpercentile(boots, 97.5)))
    res["per_participant"] = [{"participant": p, "n": len(v := [r for r in win if r["participant"] == p]),
                               "v2_win_share": sum(r["choice"] == r["v2_side"] for r in v) / max(len(v), 1)}
                              for p in people]
    # khớp hành vi thật: top-1 của danh sách được chọn có phải sản phẩm khách thật sự mua?
    hit = {"human_choice": [], "v2": [], "popularity": []}
    for r in rs:
        t = T[r["task_id"]]
        bought = set(json.loads(t["bought"]))
        if not bought:
            continue
        v2, pop = json.loads(t["list_v2"]), json.loads(t["list_pop"])
        hit["v2"].append(bool(v2) and v2[0] in bought)
        hit["popularity"].append(bool(pop) and pop[0] in bought)
        if r["choice"] in ("A", "B"):
            chosen = v2 if r["choice"] == r["v2_side"] else pop
            hit["human_choice"].append(bool(chosen) and chosen[0] in bought)
        if r.get("pitch"):
            hit.setdefault("human_pitch", []).append(r["pitch"] in bought)
    res["hit_vs_truth"] = {k: {"n": len(v), "hit@1": (sum(v) / len(v)) if v else None} for k, v in hit.items()}
    # đồng thuận trên bài chung
    by_task = {}
    for r in rs:
        if r["choice"] in ("A", "B"):
            by_task.setdefault(r["task_id"], []).append(r["choice"] == r["v2_side"])
    shared = {t: v for t, v in by_task.items() if len(v) >= 2}
    if shared:
        agree = [max(sum(v), len(v) - sum(v)) / len(v) for v in shared.values()]
        res["inter_rater"] = {"tasks": len(shared), "mean_majority_agreement": float(np.mean(agree))}
    ru = [r["reason_useful"] for r in rs if r.get("reason_useful")]
    rf = [r["reason_fits"] for r in rs if r.get("reason_fits")]
    res["reasons"] = {"n": len(ru), "useful_mean": float(np.mean(ru)) if ru else None,
                      "useful_dist": {i: ru.count(i) for i in range(1, 6)},
                      "fits": {x: rf.count(x) for x in ("yes", "partly", "no")}}
    secs = [r["seconds"] for r in rs if r.get("seconds")]
    res["seconds_median"] = float(np.median(secs)) if secs else None
    if sus_rows:
        s = [sus_score(a) for a in sus_rows]
        res["sus"] = {"n": len(s), "mean": float(np.mean(s)), "min": float(min(s)), "max": float(max(s))}
    # cỡ mẫu: số câu trả lời (không hoà) để thấy tỉ lệ thắng 0,65 so với 0,5 (alpha 0,05, power 0,8)
    res["needed_for_0.65"] = int(math.ceil(((1.96 * 0.5 + 0.8416 * math.sqrt(0.65 * 0.35)) / 0.15) ** 2))
    return res


def to_md(r: Dict) -> str:
    p = r["preference"]
    L = [f"# Pilot người dùng: hệ gợi ý v2 Berka ({time.strftime('%Y-%m-%d')})", "",
         f"{r['participants']} người tham gia, {r['responses']} câu trả lời. Quy trình: docs/RECSYS-PILOT.md.", "",
         "## 1. Sở thích mù (v2 so với độ phổ biến)", "",
         f"- v2 thắng {p['v2_wins']}, độ phổ biến thắng {p['pop_wins']}, như nhau {p['same']}.",
         f"- Tỉ lệ thắng của v2 (bỏ hoà): **{_f(p['v2_win_share'])}**, CI 95% Wilson "
         f"[{p['ci95_wilson'][0]:.3f}; {p['ci95_wilson'][1]:.3f}]"
         + (f", CI 95% bootstrap theo người [{p['ci95_cluster_bootstrap'][0]:.3f}; {p['ci95_cluster_bootstrap'][1]:.3f}]"
            if "ci95_cluster_bootstrap" in p else "") + f"; kiểm định dấu p = {p['p_sign_test']:.4f}.",
         f"- Cần khoảng {r['needed_for_0.65']} câu trả lời không hoà để phát hiện tỉ lệ thắng 0,65 (alpha 0,05, power 0,8).",
         "", "| Người | Số bài | Tỉ lệ thắng của v2 |", "|---|---:|---:|"]
    L += [f"| {x['participant']} | {x['n']} | {x['v2_win_share']:.2f} |" for x in r["per_participant"]]
    L += ["", "## 2. Khớp hành vi mua thật (chỉ khách có mua trong 3 tháng sau)", "",
          "| Ai chọn | Số bài | Top-1 trúng sản phẩm khách mua |", "|---|---:|---:|"]
    names = {"human_choice": "Danh sách người tham gia chọn", "v2": "v2", "popularity": "Độ phổ biến",
             "human_pitch": "Sản phẩm người tham gia sẽ chào"}
    L += [f"| {names.get(k, k)} | {v['n']} | {_f(v['hit@1'])} |" for k, v in r["hit_vs_truth"].items()]
    if "inter_rater" in r:
        L += ["", f"Đồng thuận trên {r['inter_rater']['tasks']} bài chung: trung bình "
              f"{r['inter_rater']['mean_majority_agreement']:.0%} người chọn giống đa số."]
    rs = r["reasons"]
    L += ["", "## 3. Lời giải thích của v2", "",
          f"Có ích cho RM (1-5): trung bình {_f(rs['useful_mean'], 2)} trên {rs['n']} lượt; phân bố {rs['useful_dist']}. "
          f"Khớp hồ sơ: có {rs['fits']['yes']}, một phần {rs['fits']['partly']}, không {rs['fits']['no']}.", ""]
    if "sus" in r:
        s = r["sus"]
        L += ["## 4. Khả dụng (SUS)", "",
              f"SUS trung bình {s['mean']:.1f} (n = {s['n']}, thấp nhất {s['min']:.1f}, cao nhất {s['max']:.1f}); "
              "68 là mức trung bình của các hệ thống được khảo sát, trên 80 là tốt.", ""]
    L += [f"Thời gian trung vị mỗi bài: {_f(r['seconds_median'], 1)} giây.", ""]
    return "\n".join(L)


def _f(v, nd=3):
    return "-" if v is None else f"{v:.{nd}f}"


def main(argv=None) -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["build", "analyze"])
    ap.add_argument("--participants", type=int, default=8)
    ap.add_argument("--sus", help="CSV: participant,q1..q10 (thang 1-5)")
    a = ap.parse_args(argv)
    if a.cmd == "build":
        build(a.participants)
        return 0
    import pandas as pd

    from .run import REPORTS

    sus = None
    if a.sus:
        s = pd.read_csv(a.sus)
        sus = s[[f"q{i}" for i in range(1, 11)]].astype(int).values.tolist()
    res = analyze(read_responses(), pd.read_parquet(TASKS), sus)
    out = REPORTS / "berka"
    (out / "pilot.json").write_text(json.dumps(res, ensure_ascii=False, indent=1, default=float), encoding="utf-8")
    (out / "pilot.md").write_text(to_md(res), encoding="utf-8")
    print(f"  bao cao {out / 'pilot.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
