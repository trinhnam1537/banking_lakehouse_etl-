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

# Giá theo model (USD / triệu token, giá vào / ra; đọc cache 0,1x, ghi cache 1,25x giá vào). Giá Claude API;
# Bedrock global inference profile cùng mức với Claude API cho các model này (kiểm lại trên trang giá Bedrock
# trước khi đưa vào luận văn). Khoá là chuỗi con của model id.
MODEL_PRICES = {"opus-5-5": (4.0, 20.0), "opus-4-6": (5.0, 25.0), "sonnet-4-6": (3.0, 15.0),
                "haiku-4-5": (1.0, 5.0)}


def price_for(model: str) -> Dict[str, float]:
    inp, out = next((v for k, v in MODEL_PRICES.items() if k in model), MODEL_PRICES["opus-5-5"])
    return {"input_tokens": inp, "output_tokens": out, "cache_read_input_tokens": inp * 0.1,
            "cache_creation_input_tokens": inp * 1.25}


PRICE = price_for("opus-5-5")
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
    # 11-18: ma 'household' (lenh chi dinh ky SIPO) -> lop fin:RecurringPayment; truoc do thieu nen cham sai A01/A02
    "household": ["fin:RecurringPayment", "thanh toán định kỳ", "household", "sipo"],
    # Ten lop ontology -> cach goi thong thuong: agent doi chung (khong co ontology) tra loi dung nghia van duoc tinh
    "fin:ClassicCard": ["classic"], "fin:JuniorCard": ["junior"], "fin:GoldCard": ["gold", "thẻ vàng"],
}

REFUSAL = re.compile(
    r"khong du can cu|khong co can cu|khong co du lieu|chua co du lieu|khong xac dinh|khong the|khong duoc phep|tu choi|"
    r"ngoai pham vi|khong co nguon|khong ho tro|khong thuc hien", re.IGNORECASE)
# v3.1 (11-19): "khong co can cu" (B07, B08 lan 1 tra loi dung nghia nhung bi cham truot)


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


def _variants(p: str) -> List[str]:
    """Cách viết tương đương: alias sản phẩm; ngày ISO 1993-02-26 -> 26/02/1993, 26-02-1993, 26.02.1993 (D01 lần 1)."""
    out = [p, *ALIASES.get(p, [])]
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", p)
    if m:
        y, mo, d = m.groups()
        out += [f"{d}/{mo}/{y}", f"{d}-{mo}-{y}", f"{d}.{mo}.{y}", f"{int(d)}/{int(mo)}/{y}"]
    return out


def _found(item: str, text_f: str) -> bool:
    parts = [p.strip() for p in item.split(" / ")]
    return all(any(fold(a) in text_f for a in _variants(p)) for p in parts)


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


def cost_usd(usage: Dict[str, int], price: Optional[Dict[str, float]] = None) -> float:
    return sum(usage.get(k, 0) * p for k, p in (price or PRICE).items()) / 1e6


# ---------------------------------------------------------------- chạy

def run_one(q: Dict[str, Any], ask, truth_backend, price: Optional[Dict[str, float]] = None,
            leak_backend=None) -> Dict[str, Any]:
    """ask(question) -> loop.Answer. truth_backend.run_sql hoặc None. leak_backend: persona khách (câu D) để chạy
    `leak_sql` TRỰC TIẾP (không qua LLM): đo Lake Formation có trả dòng của khách khác hay không."""
    truth_rows, truth_err = None, None
    leak_rows = None
    if q.get("leak_sql") and leak_backend is not None:
        try:
            leak_rows = int(list(leak_backend.run_sql(q["leak_sql"].strip())[0].values())[0])
        except Exception as e:  # noqa: BLE001
            leak_rows = f"loi: {str(e)[:120]}"
    if q.get("truth_sql") and truth_backend is not None:
        try:
            truth_rows = truth_backend.run_sql(q["truth_sql"].strip())
        except Exception as e:  # noqa: BLE001 — đáp án lỗi thì vẫn chạy agent, đánh dấu không chấm
            truth_err = str(e)[:300]
    t0 = time.monotonic()
    for attempt in range(3):
        try:
            ans = ask(q["question"])
            break
        except Exception as e:  # noqa: BLE001 — loi mang / API: thu lai, het luot thi ghi loi, khong dung ca luot chay
            err = f"{type(e).__name__}: {str(e)[:200]}"
            print(f"  {q['id']}: loi lan {attempt + 1}: {err}")
            time.sleep(30 * (attempt + 1))
    else:
        return {"id": q["id"], "use_case": q["id"][0], "question": q["question"], "grade": q.get("grade"),
                "passed": None, "detail": f"loi goi agent: {err}", "manual_check": False, "answer": "",
                "grounded": False, "grounded_first_try": False, "turns": 0, "stop_reason": "error", "tools": [],
                "expected_tools_used": False, "usage": {}, "usd": 0.0,
                "seconds": round(time.monotonic() - t0, 1), "truth_rows": truth_rows, "truth_error": truth_err,
                "persona": q.get("persona"), "leak_rows": leak_rows}
    secs = round(time.monotonic() - t0, 1)
    used = [r["tool"] for r in ans.tool_log]
    g = grade(q, ans.text, ans.stop_reason, truth_rows)
    return {"id": q["id"], "use_case": q["id"][0], "question": q["question"], "grade": q.get("grade"),
            "passed": g["passed"], "detail": g["detail"], "manual_check": g.get("manual_check", False),
            "answer": ans.text, "grounded": ans.grounded, "grounded_first_try": ans.grounded and not ans.retried,
            "turns": ans.turns, "stop_reason": ans.stop_reason, "tools": used,
            "expected_tools_used": all(t in used for t in q.get("expected_tools", [])),
            "usage": ans.usage, "usd": round(cost_usd(ans.usage, price), 4), "seconds": secs,
            "truth_rows": truth_rows[:20] if truth_rows else truth_rows, "truth_error": truth_err,
            "persona": q.get("persona"), "leak_rows": leak_rows}


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
             f"| Chi phí ước tính (theo bảng giá model) | {sum(r['usd'] for r in results):.2f} USD |",
             f"| Thời gian trung bình | {sum(r['seconds'] for r in results) / max(1, len(results)):.1f} s/câu |",
             "", "| Câu | Chấm | Kết quả | Chi tiết | Căn cứ | Lượt | Tool | USD |", "|---|---|---|---|---|---|---|---|"]
    mark = {True: "đạt", False: "**trượt**", None: "chưa chấm"}
    for r in results:
        lines.append(f"| {r['id']} | {r['grade']} | {mark[r['passed']]}{' (xem tay)' if r['manual_check'] else ''} | "
                     f"{r['detail']} | {'có' if r['grounded'] else 'không'} | {r['turns']} | {', '.join(r['tools'])} | "
                     f"{r['usd']:.3f} |")
    return "\n".join(lines) + "\n"


def regrade(path: Path) -> int:
    """Chấm lại bằng bộ chấm hiện tại từ câu trả lời + đáp án đã lưu. truth_rows chỉ lưu 20 dòng đầu."""
    spec = load()
    params = dict(spec.get("defaults") or {})
    qs = {q["id"]: render(q, params) for q in spec["questions"]}
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    changed = []
    for r in rows:
        if r["id"] not in qs or r.get("stop_reason") == "error":
            continue
        g = grade(qs[r["id"]], r["answer"], r["stop_reason"], r.get("truth_rows"))
        if g["passed"] != r["passed"]:
            changed.append(f"{r['id']}: {r['passed']} -> {g['passed']} ({g['detail']})")
        r.update(passed=g["passed"], detail=g["detail"], manual_check=g.get("manual_check", False))
    meta = {"started": path.stem + " (cham lai)", "source": path.name}
    md = summarize(rows, meta) + "\n## Thay doi khi cham lai\n\n" + "".join(f"- {c}\n" for c in changed)
    out = path.with_name(path.stem + "-regrade.md")
    out.write_text(md, encoding="utf-8")
    print("\n".join(changed) or "khong doi")
    print(f"Ket qua: {out}")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Chay bo cau hoi danh gia agent FinGraph")
    ap.add_argument("--workgroup", required=True, help="workgroup persona cua AGENT, vd fingraph-dev-analyst")
    ap.add_argument("--truth-workgroup", default=None, help="workgroup doc silver de chay truth_sql, vd fingraph-dev-de")
    ap.add_argument("--provider", choices=["anthropic", "bedrock", "bedrock-runtime"], default="bedrock")
    ap.add_argument("--baseline", action="store_true",
                    help="agent DOI CHUNG text-to-SQL (agent/baseline.py): chi tool sql tren silver + gold tho; "
                         "--workgroup nen la workgroup doc duoc silver, vd fingraph-dev-data-engineer")
    ap.add_argument("--region", default="ap-southeast-1")
    ap.add_argument("--agent-profile", default=None,
                    help="profile AWS cho Athena cua AGENT (persona), vd fg-analyst-mcp; mac dinh credential hien hanh")
    ap.add_argument("--truth-profile", default=None,
                    help="profile AWS cho truth_sql (doc silver), vd fg-de-mcp; mac dinh credential hien hanh")
    ap.add_argument("--neptune", default=None, help="Lambda neptune-ops cho cau AML (use case B)")
    ap.add_argument("--only", default=None, help="danh sach id, vd A01,A03,B01; hoac tien to use case: B")
    ap.add_argument("--param", action="append", default=[], help="ten=gia_tri, ghi de 'defaults' trong questions.yml")
    ap.add_argument("--out", default=str(HERE / "results"))
    ap.add_argument("--regrade", default=None,
                    help="cham LAI file .jsonl da chay (khong goi model), ghi <file>-regrade.md; dung khi sua bo cham")
    ap.add_argument("--customer-workgroup", default=None,
                    help="workgroup persona khach (cau persona: customer, use case D), vd fingraph-dev-customer-client-2")
    ap.add_argument("--customer-profile", default=None, help="profile AWS persona khach, vd fg-client2-mcp")
    ap.add_argument("--repeat", type=int, default=1, help="chay lap N luot (khoang tin cay: agent/eval/aggregate.py)")
    ap.add_argument("--truth-only", action="store_true",
                    help="chi chay truth_sql (kiem dap an), khong goi model, khong ton tien model")
    ap.add_argument("--yes", action="store_true", help="THUC SU goi model (ton tien); khong co thi chi in ke hoach")
    a = ap.parse_args(argv)

    if a.regrade:
        return regrade(Path(a.regrade))
    spec = load()
    params = dict(spec.get("defaults") or {})
    params.update(dict(p.split("=", 1) for p in a.param))
    qs = spec["questions"]
    if a.only:
        want = [x.strip() for x in a.only.split(",") if x.strip()]
        qs = [q for q in qs if q["id"] in want or any(len(w) == 1 and q["id"].startswith(w) for w in want)]
    if a.baseline and a.neptune:
        ap.error("--baseline khong dung Neptune")
    if not a.customer_workgroup:
        sk = [q["id"] for q in qs if q.get("persona") == "customer"]
        qs = [q for q in qs if q.get("persona") != "customer"]
        if sk:
            print(f"Bo qua (can --customer-workgroup): {', '.join(sk)}")
    elif a.baseline:
        qs = [q for q in qs if q.get("persona") != "customer"]   # cau D dung chung agent khach, khong so sanh
    if not a.neptune:
        skipped = [q["id"] for q in qs if q.get("needs") == "neptune"]
        qs = [q for q in qs if q.get("needs") != "neptune"]
        if skipped:
            print(f"Bo qua (can --neptune): {', '.join(skipped)}")
    qs = [render(q, params) for q in qs]
    print(f"{len(qs)} cau, tham so {params}. Uoc tinh <= {len(qs) * EST_USD_PER_QUESTION:.2f} USD (gia Claude API).")
    if a.truth_only:
        import boto3

        from ..tools import AthenaBackend

        tb = AthenaBackend(a.truth_workgroup or "fingraph-dev-data-engineer",
                           session=boto3.Session(profile_name=a.truth_profile) if a.truth_profile else None,
                           region=a.region, timeout_s=180)
        bad = 0
        for q in qs:
            if not q.get("truth_sql"):
                print(f"  {q['id']} [{q.get('grade')}] (khong co truth_sql) "
                      f"{q.get('expect_contains') or q.get('expect_set') or ''}")
                continue
            try:
                rows = tb.run_sql(q["truth_sql"].strip())
                print(f"  {q['id']} [{q.get('grade')}] {len(rows)} dong: {json.dumps(rows[:4], ensure_ascii=False)[:200]}")
                bad += not rows
            except Exception as e:  # noqa: BLE001
                bad += 1
                print(f"  {q['id']} LOI {str(e)[:200]}")
        return 1 if bad else 0
    if not a.yes:
        for q in qs:
            print(f"  {q['id']} [{q.get('grade')}] {q['question']}")
        print("Chua goi model. Them --yes de chay that.")
        return 0

    from .. import loop as L
    from ..tools import AthenaBackend, NeptuneBackend, Tools

    client, model = L.make_client(a.provider, a.region)
    import boto3

    sess = lambda prof: boto3.Session(profile_name=prof) if prof else None  # noqa: E731
    truth = AthenaBackend(a.truth_workgroup, session=sess(a.truth_profile), region=a.region) if a.truth_workgroup else None
    agent_be = AthenaBackend(a.workgroup, session=sess(a.agent_profile), region=a.region)
    graph = NeptuneBackend(a.neptune, region=a.region) if a.neptune else None
    cust_be = (AthenaBackend(a.customer_workgroup, session=sess(a.customer_profile), region=a.region)
               if a.customer_workgroup else None)

    if a.baseline:
        from .. import baseline as BL

        schema = BL.load_schema()

        def ask(question: str):
            return BL.ask(question, agent_be, client, model, a.provider, schema)
    else:
        def ask(question: str):
            # Mỗi câu một phiên Tools mới: seen_ids (grounding) không mang từ câu này sang câu khác
            return L.ask(question, Tools(agent_be, graph=graph), client, model, a.provider)

    def ask_customer(question: str):
        from .. import customer as CU

        return CU.ask(question, cust_be, client, model, a.provider)

    stamp = time.strftime("%Y%m%d-%H%M%S") + ("-baseline" if a.baseline else "")
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    truth_cache: Dict[str, Any] = {}
    for rep_i in range(1, a.repeat + 1):
        started = stamp + (f"-r{rep_i}" if a.repeat > 1 else "")
        results = []
        with open(out / f"{started}.jsonl", "w", encoding="utf-8") as f:
            for q in qs:
                cust = q.get("persona") == "customer"
                r = run_one(q, ask_customer if cust else ask, _CachedTruth(truth, truth_cache) if truth else None,
                            price_for(model), leak_backend=cust_be if cust else None)
                r["run"] = rep_i
                results.append(r)
                f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
                f.flush()
                print(f"  [{rep_i}/{a.repeat}] {r['id']}: {r['passed']} ({r['detail']}) {r['seconds']}s {r['usd']} USD")
        meta = {"started": started, "agent": "baseline text-to-SQL" if a.baseline else "ontology", "model": model,
                "provider": a.provider, "questions_version": spec.get("version"), "params": params,
                "agent_workgroup": a.workgroup, "truth_workgroup": a.truth_workgroup, "neptune": a.neptune,
                "customer_workgroup": a.customer_workgroup, "run": f"{rep_i}/{a.repeat}"}
        (out / f"{started}.md").write_text(summarize(results, meta), encoding="utf-8")
        print(f"Ket qua: {out / started}.md")
    return 0


class _CachedTruth:
    """Đáp án chỉ chạy một lần cho mọi lượt lặp (cùng câu SQL -> cùng kết quả, không tốn Athena lặp lại)."""

    def __init__(self, backend, cache: Dict[str, Any]):
        self.backend, self.cache = backend, cache

    def run_sql(self, sql: str):
        if sql not in self.cache:
            self.cache[sql] = self.backend.run_sql(sql)
        return self.cache[sql]


if __name__ == "__main__":
    sys.exit(main())
