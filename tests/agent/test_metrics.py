"""Tầng chỉ số của agent (agent/metrics.py): whitelist tham số, SQL đọc bản hiện hành, trích dẫn qua grounding."""

import pytest

from agent import catalog as C
from agent import grounding as G
from agent import metrics as MT
from agent.tools import TOOL_SPECS, Tools


@pytest.fixture(scope="module")
def cat():
    return C.load()


class FakeBackend:
    def __init__(self, rows=None):
        self.rows = rows if rows is not None else [{"job": "student", "subscription_rate": "0.30006"}]
        self.sql = []

    def run_sql(self, sql):
        self.sql.append(sql)
        return self.rows


def test_moi_chi_so_sinh_sql_hop_le_va_chi_doc():
    for name, m in MT.METRICS.items():
        sql = MT.build_sql(name, list(m.dims)[:1], None)
        assert sql.startswith("SELECT ") and sql.endswith("LIMIT 200")
        for t in m.source_tables:
            assert t in sql
        if any(t.startswith("silver_") for t in m.source_tables):
            assert "ds_record_status" in sql and "row_number()" in sql      # bản hiện hành


@pytest.mark.parametrize("by,filters", [
    (["job; DROP TABLE x"], None),
    (["job", "marital", "education"], None),
    (["khong_co"], None),
    (["job"], {"job": "admin.' OR '1'='1"}),
    (["job"], {"khong_co": "x"}),
])
def test_tham_so_ngoai_whitelist_bi_chan(by, filters):
    with pytest.raises(MT.MetricError):
        MT.build_sql("bank_marketing_subscription", by, filters)


def test_bo_loc_dang_co_va_gia_tri():
    sql = MT.build_sql("bank_marketing_subscription", ["job"], {"exclude_unknown_job": "true", "dataset": "full"})
    assert "job <> 'unknown'" in sql and "dataset = 'full'" in sql and "GROUP BY 1" in sql


def test_bang_da_tong_hop_khong_gop_va_co_bo_loc_mac_dinh():
    sql = MT.build_sql("card_fraud_rules", None, None)
    assert "GROUP BY" not in sql and "max(" not in sql and "severity = 'any'" in sql
    assert "severity = 'high'" in MT.build_sql("card_fraud_rules", None, {"severity": "high"})


def test_cite_id_on_dinh_va_doi_theo_tham_so():
    a = MT.cite_id("berka_loans", ["region"], None)
    assert a == MT.cite_id("berka_loans", ["region"], {}) and len(a) == 32
    assert a != MT.cite_id("berka_loans", ["district"], None)


def test_tool_metric_ghi_cite_id_cho_grounding(cat):
    be = FakeBackend()
    tools = Tools(be, cat=cat)
    out = tools.call("metric", {"metric": "bank_marketing_subscription", "by": ["job"],
                                "filters": [{"name": "exclude_unknown_job", "value": "true"}]})
    assert "_is_error" not in out and out["source_tables"]
    cid = out["cite_id"]
    assert G.check(f"Sinh viên đăng ký 30,0% [metric:{cid}] (silver_bank_marketing).", tools.seen_ids).ok
    assert not G.check(f"Sinh viên 30,0% [metric:{'0' * 32}]", tools.seen_ids).ok


def test_tool_metric_tham_so_sai_tra_is_error(cat):
    out = Tools(FakeBackend(), cat=cat).call("metric", {"metric": "berka_loans", "by": ["khong_co"], "filters": None})
    assert out.get("_is_error") and "khong hop le" in out["error"]


def test_spec_tool_metric_strict():
    spec = next(t for t in TOOL_SPECS if t["name"] == "metric")
    assert spec["strict"] and set(spec["input_schema"]["required"]) == {"metric", "by", "filters"}
    assert spec["input_schema"]["properties"]["metric"]["enum"] == sorted(MT.METRICS)


def test_nhat_ky_tool_ghi_nguoi_hoi_persona_va_id(cat, tmp_path, monkeypatch):
    """Checklist O11: mọi lời gọi tool ghi log (người hỏi, persona, tool, id trả về), ghi được ra file kiểm toán."""
    audit = tmp_path / "audit.jsonl"
    monkeypatch.setenv("FINGRAPH_AGENT_AUDIT", str(audit))
    be = FakeBackend(rows=[{"uid": "a" * 32, "label": "Khach 2"}])
    be.workgroup = "fingraph-dev-analyst"
    recs = []
    tools = Tools(be, cat=cat, log=recs.append, asker="rm01")
    tools.call("find_entity", {"text": "Khach 2", "class_iri": None, "limit": 5})
    tools.call("find_entity", {"text": "", "class_iri": None, "limit": 5})          # loi tham so cung ghi
    assert [r["ok"] for r in recs] == [True, False]
    r = recs[0]
    assert r["asker"] == "rm01" and r["persona"] == "fingraph-dev-analyst" and r["ids"] == ["a" * 32]
    assert len(audit.read_text(encoding="utf-8").splitlines()) == 2
