"""Agent (O11) không cần AWS / Claude thật: catalog, chặn SQL injection, grounding, vòng lặp với
client giả (trả đúng hình dạng response của Messages API: stop_reason + content blocks)."""

from __future__ import annotations

import json
from types import SimpleNamespace as NS

import pytest

from agent import catalog as C
from agent import grounding as G
from agent import loop as L
from agent import tools as TL

U1 = "a" * 32
T1 = "b" * 32


@pytest.fixture(scope="module")
def cat():
    return C.build()


def test_catalog_commit_khop_tbox_va_mapping():
    """Sửa TBox/mapping mà quên sinh lại catalog -> đỏ (checklist O11)."""
    assert C.main(["--check"]) == 0


def test_catalog_on_dinh_va_khong_co_du_lieu_khach(cat):
    a, b = C.prompt_text(cat), C.prompt_text(C.build())
    assert a == b                                   # ổn định từng byte -> prompt caching
    assert "fin:CreditCard | Thẻ tín dụng" in a
    assert "client_id=" not in a and "Khách 1" not in a
    loan = next(c for c in cat["classes"] if c["iri"] == "fin:Loan")
    assert {"berka", "corebank"} <= set(loan["graph_sources"])
    credit = next(c for c in cat["classes"] if c["iri"] == "fin:CreditCard")
    assert {"berka", "corebank"} <= set(credit["graph_sources"])   # lan tu lop con classic/gold
    santa = next(c for c in cat["classes"] if c["iri"] == "fin:Fund")
    assert santa["graph_sources"] == []             # Santander chua co -> agent biet la chua co du lieu


def test_describe_theo_nhan_ma_nguon(cat):
    assert C.describe(cat, "thẻ tín dụng")[0]["iri"] == "fin:CreditCard"
    assert C.describe(cat, "santander:ind_tjcr_fin_ult1")[0]["iri"] == "fin:CreditCard"
    assert C.describe(cat, "fin:holdsAccount")[0]["type"] == "property"


@pytest.mark.parametrize("bad", ["x' OR '1'='1", "a" * 31, "A" * 32, "", None])
def test_uid_xau_bi_chan_truoc_khi_vao_sql(bad):
    with pytest.raises(TL.ToolError):
        TL.sql_customer_360(bad, None)


def test_tham_so_lop_va_ngay_bi_kiem():
    with pytest.raises(TL.ToolError):
        TL.sql_count_holders("fin:Loan'; DROP TABLE x --", None, None)
    with pytest.raises(TL.ToolError):
        TL.sql_customer_360(U1, "2024-01-01' OR 1=1")
    sql = TL.sql_find_entity("O'Brien", None, 999)
    assert "o''brien" in sql and sql.endswith("LIMIT 50")


@pytest.mark.parametrize("sql", [
    "DELETE FROM track_a_onto.entity",
    "SELECT 1; DROP TABLE track_a_onto.entity",
    "SELECT * FROM silver_corebank.client",          # PII o silver: ngoai pham vi tool query
    "WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x",
])
def test_query_chi_doc_va_chi_db_cho_phep(sql):
    with pytest.raises(TL.ToolError):
        TL.check_readonly_sql(sql)


def test_query_hop_le_duoc_them_limit():
    s = TL.check_readonly_sql("SELECT class_iri, count(*) FROM track_a_onto.entity GROUP BY 1")
    assert s.endswith(f"LIMIT {TL.MAX_ROWS}")


def test_grounding():
    assert G.check(f"Khách 1 giữ thẻ [triple:{T1}]", {T1}).ok
    r = G.check(f"Khách 1 giữ thẻ [triple:{'c' * 32}]", {T1})
    assert not r.ok and r.unknown == ["c" * 32]
    assert not G.check("Khách 1 giữ 3 sản phẩm", {T1}).ok      # co du lieu ma khong trich
    assert G.check("Không đủ căn cứ để trả lời.", set()).ok


# ---------------------------------------------------------------- vòng lặp với client giả

class FakeBackend:
    def __init__(self):
        self.sql = []

    def run_sql(self, sql):
        self.sql.append(sql)
        if "lower(label) LIKE" in sql:
            return [{"uid": U1, "class_iri": "fin:Customer", "label": "Khách 2", "source_system": "berka"}]
        if "FROM track_a_onto.entity WHERE uid" in sql:
            return [{"uid": U1, "class_iri": "fin:Customer", "label": "Khách 2"}]
        return [{"kind": "holding", "triple_id": T1, "item_class": "fin:Loan", "item_label": "Khoản vay",
                 "valid_from": "1996-06-01"}]


class FakeClient:
    """Kịch bản: find_entity -> customer_360 -> trả lời (lần đầu trích sai id, lần hai đúng)."""

    def __init__(self, answers):
        self.calls = []
        self.answers = list(answers)
        self.messages = self
        self.beta = NS(messages=self)

    def create(self, **kw):
        self.calls.append(kw)
        return self.answers.pop(0)


def _tool(name, args, i):
    return NS(stop_reason="tool_use", content=[NS(type="tool_use", id=f"tu{i}", name=name, input=args)])


def _text(t):
    return NS(stop_reason="end_turn", content=[NS(type="text", text=t)])


def test_vong_lap_goi_tool_kiem_grounding_va_cho_sua_mot_lan(cat):
    client = FakeClient([
        _tool("find_entity", {"text": "Khách 2", "class_iri": "fin:Customer", "limit": None}, 1),
        _tool("customer_360", {"uid": U1, "as_of": None}, 2),
        _text(f"Khách 2 có khoản vay từ 1996-06-01 [triple:{'d' * 32}]"),   # id bia -> bi chan
        _text(f"Khách 2 [uid:{U1}] có khoản vay từ 1996-06-01 [triple:{T1}]"),
    ])
    be = FakeBackend()
    ans = L.ask("Khách 2 giữ gì?", TL.Tools(be, cat), client, L.MODEL, provider="anthropic")
    assert ans.grounded and ans.turns == 4 and T1 in ans.text
    assert [r["tool"] for r in ans.tool_log] == ["find_entity", "customer_360"]
    first, last = client.calls[0], client.calls[-1]
    # system + tools giong het nhau moi luot (cache), lich su chi append
    assert first["system"] == last["system"] and first["tools"] is last["tools"]
    assert last["messages"][:len(first["messages"])] == first["messages"]
    assert first["fallbacks"] == "default" and first["output_config"] == {"effort": "medium"}
    tool_results = last["messages"][2]["content"]
    assert tool_results[0]["type"] == "tool_result" and json.loads(tool_results[0]["content"])["rows"][0]["uid"] == U1


def test_tham_so_sai_tra_ve_is_error_de_model_tu_sua(cat):
    client = FakeClient([_tool("customer_360", {"uid": "Khách 2", "as_of": None}, 1),
                         _text("Không đủ căn cứ để trả lời.")])
    ans = L.ask("?", TL.Tools(FakeBackend(), cat), client, L.MODEL, provider="bedrock")
    res = client.calls[1]["messages"][2]["content"][0]
    assert res["is_error"] and "find_entity" in json.loads(res["content"])["error"]
    assert "fallbacks" not in client.calls[0]          # Bedrock: khong co tham so fallbacks
    assert ans.grounded


# ---------------------------------------------------------------- tool AML trên Neptune (backend giả)

class FakeGraph:
    """Đồ thị giả: A -(1 bước)- B không có; A -(2 bước qua P)- B có. Ghi lại truy vấn để kiểm tham số hoá."""

    def __init__(self):
        self.calls = []

    def run_cypher(self, query, params):
        self.calls.append((query, params))
        if "*1]" in query or "*1..1}" in query:
            return []
        if "-[:" in query and "*2]" in query:
            return [{"nodes": [{"uid": U1, "label": "TK AML 1-A1"}, {"uid": "c" * 32, "label": "Corporation #1"},
                               {"uid": "d" * 32, "label": "TK AML 2-B1"}],
                     "edges": [{"triple_id": T1, "type": "isHeldBy"}, {"triple_id": "e" * 32, "type": "isHeldBy"}]}]
        if "involvedIn]->(v)" in query:
            return [{"label": "TK AML 1-A1", "attempts": [{"attempt_uid": "f" * 32, "typology": "CYCLE",
                                                           "triple_id": "0" * 32}], "owners": []}]
        return [{"triple_id": "1" * 32, "from_uid": U1, "to_uid": "d" * 32, "n_txn": 3}]


def test_explain_link_tang_dan_do_dai_va_tham_so_hoa(cat):
    g = FakeGraph()
    t = TL.Tools(FakeBackend(), cat, graph=g)
    out = t.call("explain_link", {"uid_a": U1, "uid_b": "d" * 32, "max_hops": 4})
    assert out["hops"] == 2 and len(out["rows"][0]["edges"]) == 2
    assert len(g.calls) == 2                                    # 1 buoc khong thay -> thu 2 buoc
    assert all(U1 not in q and p == {"a": U1, "b": "d" * 32} for q, p in g.calls)   # uid KHONG ghep vao chuoi
    assert {T1, "e" * 32, "c" * 32} <= t.seen_ids              # id long nhau vao danh sach grounding
    assert G.check(f"Hai tai khoan cung chu [triple:{T1}] [triple:{'e' * 32}]", t.seen_ids).ok


def test_aml_subgraph_va_loi_tham_so(cat):
    g = FakeGraph()
    t = TL.Tools(FakeBackend(), cat, graph=g)
    out = t.call("aml_subgraph", {"uid": U1, "hops": 9, "limit": 10_000})
    assert out["node"]["attempts"][0]["typology"] == "CYCLE"
    q, p = g.calls[-1]
    assert "*1..2]" in q and p["limit"] == 200                  # hops, limit bi kep bien
    assert "f" * 32 in t.seen_ids and "1" * 32 in t.seen_ids
    bad = t.call("aml_subgraph", {"uid": "' OR 1=1 //", "hops": 1, "limit": 5})
    assert bad["_is_error"] and len(g.calls) == 2              # chan truoc khi goi Neptune
    off = TL.Tools(FakeBackend(), cat).call("explain_link", {"uid_a": U1, "uid_b": U1, "max_hops": 2})
    assert off["_is_error"] and "Neptune chua bat" in off["error"]
