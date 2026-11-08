"""Chạy bộ câu hỏi đánh giá agent (bước O12, agent/README.md mục A7; chi tiết docs/AI-AGENT.md mục 9).

    # Xem kế hoạch + ước chi phí, KHÔNG gọi model (mặc định)
    python -m agent.eval.run --workgroup fingraph-dev-analyst
    # Chạy thật (tốn tiền model): thêm --yes
    python -m agent.eval.run --workgroup fingraph-dev-analyst --truth-workgroup fingraph-dev-de \
        --neptune fingraph-dev-neptune-ops --only A01,A03,B01 --yes

Mỗi câu: (1) chạy `truth_sql` trên SILVER dưới workgroup DE -> đáp án độc lập với đồ thị; (2) hỏi agent dưới
workgroup analyst; (3) chấm TẤT ĐỊNH bằng code (không dùng LLM chấm): refuse | contains | exact_set | number.
Chấm tự động là CẬN DƯỚI có thể kiểm lại: exact_set chỉ đo độ phủ (recall) nên mọi câu exact_set được đánh dấu
`manual_check` để người chấm xem thêm độ chính xác (precision) và nội dung trích dẫn.

Kết quả: agent/eval/results/<thời điểm>.jsonl (từng câu, gồm câu trả lời, tool, token) và .md (bảng tổng hợp).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

HERE = Path(__file__).resolve().parent
QUESTIONS = HERE / "questions.yml"

# Giá Claude API cho Opus 5.5 (USD / triệu token): vào 4, ra 20; đọc cache 0,1x, ghi cache 1,25x giá vào.
# Bedrock có bảng giá riêng: kiểm lại trước khi dùng số này trong luận văn.
PRICE = {"input_tokens": 4.0, "output_tokens": 20.0, "cache_read_input_tokens": 0.4,
         "cache_creation_input_tokens": 5.0}
EST_USD_PER_QUESTION = 0.10   # cận trên ước lượng (agent/README.md A2) cho bước xem kế hoạch

# Mã sản phẩm ở gold/silver -> cách agent có thể gọi tên (IRI, nhãn tiếng Việt). Dùng cho exact_set.
ALIASES: Dict[str, List[str]] = {
    "loan": ["fin:Loan", "khoản vay", "vay"],
    "card_classic": ["fin:ClassicCard", "thẻ classic", "classic"],
    "card_junior": ["fin:JuniorCard", "thẻ junior", "junior"],
    "card_gold": ["fin:GoldCard", "thẻ vàng", "thẻ gold", "gold"],
    "permanent_order": ["fin:StandingOrder", "lệnh chuyển tiền định kỳ", "định kỳ"],
    "insurance": ["fin:InsuranceProduct", "bảo hiểm"],
    "account": ["fin:CurrentAccount", "fin:Account", "tài khoản"],
}

REFUSAL = re.compile(
    r"khong du can cu|khong co du lieu|chua co du lieu|khong xac dinh|khong the|khong duoc phep|tu choi|"
    r"ngoai pham vi|khong co nguon|khong ho tro|khong thuc hien", re.IGNORECASE)


# ---------------------------------------------------------------- nạp câu hỏi

def load(path: Path = QUESTIONS) -> Dict[str, Any]:
    import yaml

    return yaml.safe_load(path.read_text(encoding="utf-8"))


def render(q: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Thay {client_id}, {attempt_id}... trong câu hỏi và truth_sql. Thiếu tham số -> lỗi rõ ràng."""
    out = dict(q)
    for k in ("question", "truth_sql"):
        if out.get(k):
            names = set(re.findall(r"\{([a-z_]+)\}", out[k]))
            missing = names - params.keys()
            if missing:
                raise KeyError(f"{q['id']}: thieu tham so {sorted(missing)} (them --param ten=gia_tri)")
            for n in names:
                out[k] = out[k].replace("{" + n + "}", str(params[n]))
    return out


# ---------------------------------------------------------------- chấm điểm (tất định)

def fold(s: str) -> str:
    """Bỏ dấu, chữ thường: so khớp không phụ thuộc cách gõ tiếng Việt."""
    s = unicodedata.normalize("NFKD", str(s).replace("đ", "d").replace("Đ", "D"))
    return "".join(c for c in s if not unicodedata.combining(c)).lower()


def numbers_in(text: str) -> List[float]:
    """Mọi cách đọc hợp lý của các số trong câu trả lời: '1.234' (VN: nghìn), '12,5' (VN: thập phân),
    '1,234.5' (EN), '37,2%' -> 37.2 và 0.372."""
    out: List[float] = []
    for m in re.finditer(r"\d[\d.,]*\d|\d", text):
        raw = m.group(0)
        pct = text[m.end():m.end() + 2].lstrip().startswith("%")
        cands = set()
        for t in (raw.replace(".", "").replace(",", "."),   # kiểu VN
                  raw.replace(",", ""),                      # kiểu EN
                  raw):
            try:
                cands.add(float(t))
            except ValueError:
                pass
        for c in cands:
            out.append(c)
            if pct:
                out.append(c / 100)
    return out


def _num_match(t: float, cands: Iterable[float], rel: float = 0.005) -> bool:
    return any(abs(c - t) <= max(1e-9, rel * abs(t)) for c in cands)


def _truth_numbers(rows: Sequence[Dict[str, Any]]) -> List[float]:
    """Giá trị số ở cột CUỐI mỗi dòng (vd 'job, avg(...)' -> avg)."""
    vals = []
    for r in rows:
        v = list(r.values())[-1] if r else None
        try:
            vals.append(float(v))
        except (TypeError, ValueError):
            pass
    return vals


def _truth_set(rows: Sequence[Dict[str, Any]]) -> List[str]:
    """Mỗi dòng một phần tử: các cột nối bằng ' / ' (vd quận / vùng)."""
    return sorted({" / ".join(str(v) for v in r.values() if v is not None) for r in rows})


def _found(item: str, text_f: str) -> bool:
    parts = [p.strip() for p in item.split(" / ")]
    return all(any(fold(a) in text_f for a in [p, *ALIASES.get(p, [])]) for p in parts)


def grade(q: Dict[str, Any], text: str, stop_reason: str, truth_rows: Optional[Sequence[Dict[str, Any]]]
          ) -> Dict[str, Any]:
    """{passed: bool | None, detail, manual_check}. passed None = không chấm được (thiếu đáp án)."""
    kind = q.get("grade")
    tf = fold(text)
    if kind == "refuse":
        ok = stop_reason in ("refusal", "ungrounded") or bool(REFUSAL.search(tf))
        return {"passed": ok, "detail": "tu choi / noi khong du can cu" if ok else "tra loi nhu co du lieu"}
    if kind == "contains":
        need = q.get("expect_contains", [])
        miss = [x for x in need if fold(x) not in tf]
        return {"passed": not miss, "detail": f"thieu {miss}" if miss else "du"}
    if kind == "exact_set":
        items = q.get("expect_set") or (_truth_set(truth_rows) if truth_rows is not None else None)
        if items is None:
            return {"passed": None, "detail": "khong co dap an (chua chay truth_sql)", "manual_check": True}
        miss = [x for x in items if not _found(str(x), tf)]
        if not items:
            ok = bool(REFUSAL.search(tf)) or "khong co" in tf
            return {"passed": ok, "detail": "dap an rong", "manual_check": True}
        return {"passed": not miss, "detail": f"phu {len(items) - len(miss)}/{len(items)}; thieu {miss[:5]}",
                "manual_check": True}
    if kind == "number":
        if truth_rows is None:
            return {"passed": None, "detail": "khong co dap an (chua chay truth_sql)"}
        want = _truth_numbers(truth_rows)
        if not want:
            return {"passed": None, "detail": "truth_sql khong tra so"}
        got = numbers_in(text)
        hit = sum(_num_match(t, got) for t in want)
        need = len(want) if len(want) == 1 else int(len(want) * 0.8 + 0.9999)
        return {"passed": hit >= need, "detail": f"khop {hit}/{len(want)} so (can {need})"}
    return {"passed": None, "detail": f"grade {kind!r} chua ho tro"}


def cost_usd(usage: Dict[str, int]) -> float:
    return sum(usage.get(k, 0) * p for k, p in PRICE.items()) / 1e6


# ---------------------------------------------------------------- chạy

def run_one(q: Dict[str, Any], ask, truth_backend) -> Dict[str, Any]:
    """ask(question) -> loop.Answer. truth_backend.run_sql hoặc None."""
    truth_rows, truth_err = None, None
    if q.get("truth_sql") and truth_backend is not None:
        try:
            truth_rows = truth_backend.run_sql(q["truth_sql"].strip())
        except Exception as e:  # noqa: BLE001 — đáp án lỗi thì vẫn chạy agent, đánh dấu không chấm
            truth_err = str(e)[:300]
    t0 = time.monotonic()
    ans = ask(q["question"])
    secs = round(time.monotonic() - t0, 1)
    used = [r["tool"] for r in ans.tool_log]
    g = grade(q, ans.text, ans.stop_reason, truth_rows)
    return {"id": q["id"], "use_case": q["id"][0], "question": q["question"], "grade": q.get("grade"),
            "passed": g["passed"], "detail": g["detail"], "manual_check": g.get("manual_check", False),
            "answer": ans.text, "grounded": ans.grounded, "grounded_first_try": ans.grounded and not ans.retried,
            "turns": ans.turns, "stop_reason": ans.stop_reason, "tools": used,
            "expected_tools_used": all(t in used for t in q.get("expected_tools", [])),
            "usage": ans.usage, "usd": round(cost_usd(ans.usage), 4), "seconds": secs,
            "truth_rows": truth_rows[:20] if truth_rows else truth_rows, "truth_error": truth_err}


def summarize(results: List[Dict[str, Any]], meta: Dict[str, Any]) -> str:
    graded = [r for r in results if r["passed"] is not None]
    refuse = [r for r in results if r["grade"] == "refuse"]
    answered = [r for r in results if r["grade"] != "refuse"]
    pct = lambda a, b: f"{100 * a / b:.0f}% ({a}/{b})" if b else "-"  # noqa: E731
    lines = [f"# Kết quả đánh giá agent {meta['started']}", "",
             "| Thông số | Giá trị |", "|---|---|",
             *[f"| {k} | `{v}` |" for k, v in meta.items()], "",
             "| Chỉ số | Giá trị |", "|---|---|",
             f"| Độ chính xác (câu chấm được) | {pct(sum(r['passed'] for r in graded), len(graded))} |",
             f"| Có căn cứ ngay lần đầu (câu không phải refuse) | "
             f"{pct(sum(r['grounded_first_try'] for r in answered), len(answered))} |",
             f"| Từ chối đúng | {pct(sum(bool(r['passed']) for r in refuse), len(refuse))} |",
             f"| Dùng đúng tool kỳ vọng | {pct(sum(r['expected_tools_used'] for r in results), len(results))} |",
             f"| Chi phí ước tính (giá Claude API) | {sum(r['usd'] for r in results):.2f} USD |",
             f"| Thời gian trung bình | {sum(r['seconds'] for r in results) / max(1, len(results)):.1f} s/câu |",
             "", "| Câu | Chấm | Kết quả | Chi tiết | Căn cứ | Lượt | Tool | USD |", "|---|---|---|---|---|---|---|---|"]
    mark = {True: "đạt", False: "**trượt**", None: "chưa chấm"}
    for r in results:
        lines.append(f"| {r['id']} | {r['grade']} | {mark[r['passed']]}{' (xem tay)' if r['manual_check'] else ''} | "
                     f"{r['detail']} | {'có' if r['grounded'] else 'không'} | {r['turns']} | {', '.join(r['tools'])} | "
                     f"{r['usd']:.3f} |")
    return "\n".join(lines) + "\n"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Chay bo cau hoi danh gia agent FinGraph")
    ap.add_argument("--workgroup", required=True, help="workgroup persona cua AGENT, vd fingraph-dev-analyst")
    ap.add_argument("--truth-workgroup", default=None, help="workgroup doc silver de chay truth_sql, vd fingraph-dev-de")
    ap.add_argument("--provider", choices=["anthropic", "bedrock"], default="bedrock")
    ap.add_argument("--region", default="ap-southeast-1")
    ap.add_argument("--neptune", default=None, help="Lambda neptune-ops cho cau AML (use case B)")
    ap.add_argument("--only", default=None, help="danh sach id, vd A01,A03,B01; hoac tien to use case: B")
    ap.add_argument("--param", action="append", default=[], help="ten=gia_tri, ghi de 'defaults' trong questions.yml")
    ap.add_argument("--out", default=str(HERE / "results"))
    ap.add_argument("--yes", action="store_true", help="THUC SU goi model (ton tien); khong co thi chi in ke hoach")
    a = ap.parse_args(argv)

    spec = load()
    params = dict(spec.get("defaults") or {})
    params.update(dict(p.split("=", 1) for p in a.param))
    qs = spec["questions"]
    if a.only:
        want = [x.strip() for x in a.only.split(",") if x.strip()]
        qs = [q for q in qs if q["id"] in want or any(len(w) == 1 and q["id"].startswith(w) for w in want)]
    if not a.neptune:
        skipped = [q["id"] for q in qs if q.get("needs") == "neptune"]
        qs = [q for q in qs if q.get("needs") != "neptune"]
        if skipped:
            print(f"Bo qua (can --neptune): {', '.join(skipped)}")
    qs = [render(q, params) for q in qs]
    print(f"{len(qs)} cau, tham so {params}. Uoc tinh <= {len(qs) * EST_USD_PER_QUESTION:.2f} USD (gia Claude API).")
    if not a.yes:
        for q in qs:
            print(f"  {q['id']} [{q.get('grade')}] {q['question']}")
        print("Chua goi model. Them --yes de chay that.")
        return 0

    from .. import loop as L
    from ..tools import AthenaBackend, NeptuneBackend, Tools

    client, model = L.make_client(a.provider, a.region)
    truth = AthenaBackend(a.truth_workgroup, region=a.region) if a.truth_workgroup else None
    agent_be = AthenaBackend(a.workgroup, region=a.region)
    graph = NeptuneBackend(a.neptune, region=a.region) if a.neptune else None

    def ask(question: str):
        # Mỗi câu một phiên Tools mới: seen_ids (grounding) không mang từ câu này sang câu khác
        return L.ask(question, Tools(agent_be, graph=graph), client, model, a.provider)

    started = time.strftime("%Y%m%d-%H%M%S")
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    results = []
    with open(out / f"{started}.jsonl", "w", encoding="utf-8") as f:
        for q in qs:
            r = run_one(q, ask, truth)
            results.append(r)
            f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
            f.flush()
            print(f"  {r['id']}: {r['passed']} ({r['detail']}) {r['seconds']}s {r['usd']} USD")
    meta = {"started": started, "model": model, "provider": a.provider, "questions_version": spec.get("version"),
            "params": params, "agent_workgroup": a.workgroup, "truth_workgroup": a.truth_workgroup,
            "neptune": a.neptune}
    (out / f"{started}.md").write_text(summarize(results, meta), encoding="utf-8")
    print(f"Ket qua: {out / started}.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
