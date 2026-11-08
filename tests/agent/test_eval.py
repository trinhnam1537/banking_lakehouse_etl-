"""Bộ chạy đánh giá (agent/eval/run.py): chấm tất định, thay tham số, chạy với agent + đáp án giả."""

from __future__ import annotations

from types import SimpleNamespace as NS

import pytest

from agent.eval import run as E


def test_bo_cau_hoi_hop_le_va_thay_duoc_tham_so():
    spec = E.load()
    ids = [q["id"] for q in spec["questions"]]
    assert len(ids) == len(set(ids)) and any(i.startswith("B") for i in ids)
    for q in spec["questions"]:
        assert q["grade"] in ("refuse", "contains", "exact_set", "number"), q["id"]
        r = E.render(q, spec["defaults"])
        assert "{" not in r["question"] and "{" not in (r.get("truth_sql") or ""), q["id"]
        if q["grade"] in ("number",) or (q["grade"] == "exact_set" and not q.get("expect_set")):
            assert q.get("truth_sql"), f"{q['id']} can truth_sql de cham"
    with pytest.raises(KeyError):
        E.render({"id": "X", "question": "Khach {client_id}"}, {})


@pytest.mark.parametrize("text,value", [
    ("Có 1.234 khách", 1234), ("Có 1,234 khách", 1234), ("tỉ lệ 12,5%", 0.125), ("tỉ lệ 12.5 %", 12.5),
    ("đúng 7 vụ", 7), ("0,372", 0.372),
])
def test_doc_so_kieu_viet_va_kieu_anh(text, value):
    assert E._num_match(value, E.numbers_in(text))


def test_cham_number_nhieu_dong_can_80_phan_tram():
    rows = [{"job": j, "rate": r} for j, r in [("admin", "0.12"), ("student", "0.31"), ("retired", "0.25"),
                                               ("services", "0.08"), ("blue-collar", "0.07")]]
    ok = "admin 12%, student 31%, retired 25%, services 8%"            # 4/5 = 80%
    assert E.grade({"grade": "number"}, ok, "end_turn", rows)["passed"]
    assert not E.grade({"grade": "number"}, "admin 12%, student 31%", "end_turn", rows)["passed"]
    assert E.grade({"grade": "number"}, "x", "end_turn", None)["passed"] is None


def test_cham_exact_set_theo_alias_va_khong_dau():
    q = {"grade": "exact_set"}
    rows = [{"product": "loan"}, {"product": "card_gold"}]
    g = E.grade(q, "Khách 2 có khoản vay [triple:x] và thẻ vàng [triple:y]", "end_turn", rows)
    assert g["passed"] and g["manual_check"]
    assert not E.grade(q, "Khach 2 co khoan vay", "end_turn", rows)["passed"]
    rows2 = [{"district_name": "Hl.m. Praha", "region": "Prague"}]
    assert E.grade(q, "Khách ở Hl.m. Praha, vùng Prague", "end_turn", rows2)["passed"]


def test_cham_refuse_va_contains():
    assert E.grade({"grade": "refuse"}, "Không đủ căn cứ để trả lời.", "end_turn", None)["passed"]
    assert E.grade({"grade": "refuse"}, "...", "ungrounded", None)["passed"]
    assert not E.grade({"grade": "refuse"}, "Khách 2 giữ 1 quỹ đầu tư [uid:x]", "end_turn", None)["passed"]
    q = {"grade": "contains", "expect_contains": ["fin:TermDeposit"]}
    assert E.grade(q, "Cùng lớp fin:TermDeposit", "end_turn", None)["passed"]


class TruthBE:
    def run_sql(self, sql):
        assert "silver_aml" in sql
        return [{"typology": "CYCLE"}]


def test_run_one_va_bang_tong_hop():
    ans = NS(text="Vụ thuộc kiểu CYCLE [triple:" + "a" * 32 + "]", grounded=True, retried=False, turns=3,
             stop_reason="end_turn", tool_log=[{"tool": "find_entity"}, {"tool": "aml_subgraph"}],
             usage={"input_tokens": 1000, "output_tokens": 100, "cache_read_input_tokens": 10_000,
                    "cache_creation_input_tokens": 0})
    q = E.render(next(q for q in E.load()["questions"] if q["id"] == "B01"), {"attempt_id": "HI-Medium-01062"})
    r = E.run_one(q, lambda question: ans, TruthBE())
    assert r["passed"] and r["grounded_first_try"] and r["expected_tools_used"]
    assert r["usd"] == pytest.approx((1000 * 4 + 100 * 20 + 10_000 * 0.4) / 1e6, abs=1e-4)
    md = E.summarize([r], {"started": "t", "model": "m"})
    assert "| B01 | exact_set | đạt (xem tay)" in md and "100% (1/1)" in md


def test_mac_dinh_khong_goi_model(capsys):
    assert E.main(["--workgroup", "wg", "--only", "A01,B"]) == 0
    out = capsys.readouterr().out
    assert "Chua goi model" in out and "Bo qua (can --neptune): B03, B05" in out
