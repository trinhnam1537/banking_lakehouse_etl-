"""Gộp nhiều lượt đánh giá agent -> độ chính xác trung bình, khoảng tin cậy, so sánh ghép cặp hai agent.

    python -m agent.eval.aggregate --ontology 20261119-221605 [20261120-...] --baseline 20261119-221605-baseline [...]

Vì sao cần: một lượt chạy cho một con số, không biết dao động bao nhiêu (LLM không tất định: cùng câu, lượt này đạt
lượt khác trượt, vd A11 lần 1 đạt lần 2 vượt số lượt). Ở đây:
  - Mỗi lượt: độ chính xác trên câu chấm được. Nhiều lượt: trung bình ± CI 95% (t-Student theo số lượt).
  - Mỗi câu: tỉ lệ đạt qua các lượt -> câu "không ổn định" (0 < tỉ lệ < 1).
  - So sánh hai agent trên CÙNG tập câu: bootstrap theo CÂU (lấy mẫu lại câu, giữ nguyên các lượt của câu) cho
    chênh lệch độ chính xác; McNemar trên kết quả đa số của từng câu.
  - Theo use case (A-E) và theo nhóm câu (tra cứu / thống kê / phải từ chối).
Lượt có lỗi gọi model (stop_reason = error) bị bỏ câu đó, ghi số câu bị bỏ.
"""

from __future__ import annotations

import argparse
import json
import math
import pathlib
import sys
from typing import Dict, List

import numpy as np

HERE = pathlib.Path(__file__).resolve().parent
RESULTS = HERE / "results"
T95 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262}


def load_run(stem: str, regrade: bool = True) -> List[Dict]:
    """Đọc một lượt; mặc định CHẤM LẠI bằng bộ chấm hiện hành (run.grade) để mọi lượt cùng một chuẩn chấm."""
    path = RESULTS / (stem if stem.endswith(".jsonl") else f"{stem}.jsonl")
    rows = [json.loads(x) for x in path.read_text(encoding="utf-8").splitlines() if x.strip()]
    if regrade:
        from .run import grade, load, render

        spec = load()
        qs = {q["id"]: render(q, dict(spec.get("defaults") or {})) for q in spec["questions"]}
        for r in rows:
            if r["id"] in qs and r.get("stop_reason") != "error":
                r["passed"] = grade(qs[r["id"]], r["answer"], r["stop_reason"], r.get("truth_rows"))["passed"]
    return rows


def per_question(runs: List[List[Dict]]) -> Dict[str, List[int]]:
    """id câu -> danh sách kết quả 0/1 qua các lượt (bỏ lượt lỗi / không chấm được)."""
    out: Dict[str, List[int]] = {}
    for run in runs:
        for r in run:
            if r.get("stop_reason") == "error" or r.get("passed") is None:
                continue
            out.setdefault(r["id"], []).append(int(bool(r["passed"])))
    return out


def run_accuracy(run: List[Dict], ids=None) -> float:
    rs = [r for r in run if r.get("passed") is not None and r.get("stop_reason") != "error" and (ids is None or r["id"] in ids)]
    return sum(bool(r["passed"]) for r in rs) / max(len(rs), 1)


def mean_ci(xs: List[float]) -> tuple:
    m = float(np.mean(xs))
    if len(xs) < 2:
        return m, None, None
    h = T95.get(len(xs) - 1, 1.96) * float(np.std(xs, ddof=1)) / math.sqrt(len(xs))
    return m, m - h, m + h


def paired(qa: Dict[str, List[int]], qb: Dict[str, List[int]], n: int = 5000, seed: int = 0) -> Dict:
    ids = sorted(set(qa) & set(qb))
    a = np.array([np.mean(qa[i]) for i in ids])
    b = np.array([np.mean(qb[i]) for i in ids])
    rng = np.random.default_rng(seed)
    boot = [float(np.mean(a[s] - b[s])) for s in (rng.integers(0, len(ids), len(ids)) for _ in range(n))]
    ma, mb = a >= 0.5, b >= 0.5                   # kết quả đa số của câu
    b01, b10 = int(np.sum(~ma & mb)), int(np.sum(ma & ~mb))
    chi = (abs(b10 - b01) - 1) ** 2 / (b10 + b01) if b10 + b01 else 0.0
    p = math.erfc(math.sqrt(chi / 2)) if chi else 1.0
    return {"questions": len(ids), "acc_a": float(a.mean()), "acc_b": float(b.mean()), "diff": float(np.mean(a - b)),
            "ci": (float(np.percentile(boot, 2.5)), float(np.percentile(boot, 97.5))),
            "mcnemar": {"a_only": b10, "b_only": b01, "chi2": chi, "p_value": p}}


def group(qid: str, grade: str) -> str:
    return "phai tu choi" if grade == "refuse" else ("thong ke" if qid[0] == "C" or grade == "number" else "tra cuu")


def main(argv=None) -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--ontology", nargs="+", required=True)
    ap.add_argument("--baseline", nargs="*", default=[])
    ap.add_argument("--out", default=str(RESULTS / "aggregate.md"))
    a = ap.parse_args(argv)
    on = [load_run(s) for s in a.ontology]
    bl = [load_run(s) for s in a.baseline]
    meta = {r["id"]: r.get("grade") for run in on + bl for r in run}
    qo, qb = per_question(on), per_question(bl) if bl else {}
    common = set(qo) & set(qb) if bl else set(qo)
    L = ["# Tổng hợp nhiều lượt đánh giá agent", "",
         f"Agent ontology: {len(on)} lượt ({', '.join(a.ontology)}). Agent đối chứng: {len(bl)} lượt "
         f"({', '.join(a.baseline) or '-'}). Sinh bởi `python -m agent.eval.aggregate`.", "",
         "## 1. Độ chính xác theo lượt", "", "| Agent | Lượt | Trên mọi câu chấm được | Trên câu chung |",
         "|---|---|---:|---:|"]
    for name, runs, stems in (("ontology", on, a.ontology), ("đối chứng", bl, a.baseline)):
        for s, r in zip(stems, runs):
            L.append(f"| {name} | {s} | {run_accuracy(r):.1%} | {run_accuracy(r, common):.1%} |")
    L += ["", "| Agent | Trung bình (câu chung) | CI 95% theo lượt |", "|---|---:|---|"]
    for name, runs in (("ontology", on), ("đối chứng", bl)):
        if runs:
            m, lo, hi = mean_ci([run_accuracy(r, common) for r in runs])
            L.append(f"| {name} | {m:.1%} | " + (f"[{lo:.1%}; {hi:.1%}]" if lo is not None else "cần >= 2 lượt") + " |")
    if bl:
        pr = paired(qo, qb)
        mc = pr["mcnemar"]
        L += ["", "## 2. So sánh ghép cặp trên cùng câu", "",
              f"{pr['questions']} câu chung. Ontology {pr['acc_a']:.1%} vs đối chứng {pr['acc_b']:.1%}: chênh lệch "
              f"**{pr['diff']:+.1%}** (bootstrap theo câu, CI 95% [{pr['ci'][0]:+.1%}; {pr['ci'][1]:+.1%}]). McNemar trên "
              f"kết quả đa số: chỉ ontology đạt {mc['a_only']} câu, chỉ đối chứng đạt {mc['b_only']} câu, "
              f"p = {mc['p_value']:.4f}.", ""]
        L += ["## 3. Theo nhóm câu và use case", "", "| Nhóm | Số câu | Ontology | Đối chứng |", "|---|---:|---:|---:|"]
        groups: Dict[str, List[str]] = {}
        for q in common:
            groups.setdefault(group(q, meta.get(q)), []).append(q)
            groups.setdefault(f"use case {q[0]}", []).append(q)
        for gname in sorted(groups):
            qs = groups[gname]
            L.append(f"| {gname} | {len(qs)} | {np.mean([np.mean(qo[q]) for q in qs]):.0%} | "
                     f"{np.mean([np.mean(qb[q]) for q in qs]):.0%} |")
    unstable = sorted(q for q, v in qo.items() if 0 < np.mean(v) < 1)
    L += ["", "## 4. Câu không ổn định giữa các lượt (agent ontology)", "",
          ", ".join(f"{q} ({sum(qo[q])}/{len(qo[q])})" for q in unstable) or "không có", ""]
    fails = sorted(q for q, v in qo.items() if np.mean(v) == 0)
    L += ["Câu trượt ở MỌI lượt (agent ontology): " + (", ".join(fails) or "không có"), ""]
    pathlib.Path(a.out).write_text("\n".join(L), encoding="utf-8")
    print("\n".join(L))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
