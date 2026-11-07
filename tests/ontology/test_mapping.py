"""Mapping (O5) và định danh (O6): kiểm tĩnh, không cần Spark."""

from __future__ import annotations

import copy

import pytest
import yaml

from transforms.onto import ids
from transforms.onto import mapping as M
from transforms.onto import tbox as T


@pytest.fixture(scope="module")
def tb():
    return T.load()


@pytest.fixture(scope="module")
def raw_berka():
    with open(M.MAPPING_DIR / "berka.yml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def test_mappings_json_khop_yaml():
    """Glue đọc mappings.json: phải là bản biên dịch đúng của YAML (sửa YAML mà quên biên dịch -> đỏ)."""
    assert M.JSON_PATH.read_text(encoding="utf-8") == M.dumps(M.read_yaml_dir())


def test_thu_tu_cot_khoa_giu_nguyen_nhu_yaml():
    """Cạnh tham chiếu khoá node theo vị trí: bản JSON phải giữ đúng thứ tự cột khoá của YAML."""
    raw = M.read_yaml_dir()
    for m in M.load_all():
        for e in raw[m.name].get("entities") or []:
            k = e["key"]
            want = list(k) if isinstance(k, dict) else [str(x) for x in k]
            assert list(m.entities[e["id"]].key) == want, (m.name, e["id"])
    ext = M.load_all(["berka"])[0].entities["external_account"]
    assert list(ext.key) == ["bank_code", "account_no"]


def test_moi_mapping_hop_le_voi_tbox(tb):
    ms = M.load_all()
    assert {m.name for m in ms} >= {"berka", "corebank", "bank_marketing", "mbd"}
    M.validate(ms, tb)
    for m in ms:
        assert m.reviewed_by, m.name


def test_berka_va_corebank_cung_cau_truc():
    """corebank.yml sao từ berka.yml: cùng tập mục, cùng định nghĩa sản phẩm."""
    b, c = M.load_all(["berka"])[0], M.load_all(["corebank"])[0]
    assert set(b.entities) == set(c.entities)
    assert [e.id for e in b.edges] == [e.id for e in c.edges]
    assert [(h.id, h.product, h.product_notation) for h in b.holdings] == \
           [(h.id, h.product, h.product_notation) for h in c.holdings]
    assert all(t.startswith("silver_corebank.") for t in c.tables())


def test_lop_khong_co_trong_tbox_lam_do(tb, raw_berka):
    """Hàng rào O4: một mapping cố tình sai phải làm test đỏ."""
    bad = copy.deepcopy(raw_berka)
    bad["entities"][0]["class"] = "fin:KhongTonTai"
    with pytest.raises(M.MappingError, match="fin:KhongTonTai"):
        M.validate([M.parse(bad, "berka")], tb)


def test_sai_domain_range_bi_bat_khong_can_suy_dien(tb, raw_berka):
    """holdsAccount từ quận huyện: suy diễn RDFS sẽ "chữa" thành Customer; kiểm tĩnh phải báo."""
    bad = copy.deepcopy(raw_berka)
    edge = next(e for e in bad["edges"] if e["id"] == "holds_account")
    edge["subject"] = {"entity": "district", "key": ["district_id"]}
    with pytest.raises(M.MappingError, match="domain fin:holdsAccount"):
        M.validate([M.parse(bad, "berka")], tb)
    edge["subject"] = {"entity": "customer", "key": ["client_id"]}
    edge["object"] = {"entity": "region", "key": ["region"]}
    with pytest.raises(M.MappingError, match="range fin:holdsAccount"):
        M.validate([M.parse(bad, "berka")], tb)


def test_loi_cau_truc(tb, raw_berka):
    bad = copy.deepcopy(raw_berka)
    edge = next(e for e in bad["edges"] if e["id"] == "holds_account")
    edge["subject"] = {"entity": "customer", "key": ["client_id", "account_id"]}  # thừa cột khoá
    with pytest.raises(M.MappingError, match="1 cot khoa"):
        M.validate([M.parse(bad, "berka")], tb)
    bad2 = copy.deepcopy(raw_berka)
    next(e for e in bad2["edges"] if e["id"] == "holds_account").pop("source_key")
    with pytest.raises(M.MappingError, match="source_key"):
        M.validate([M.parse(bad2, "berka")], tb)
    with pytest.raises(M.MappingError, match="from"):
        M.parse({**raw_berka, "entities": [{"id": "x", "class": "fin:Country", "key": ["a"], "label": "a"}]},
                "berka")


def test_tra_ma_nguon_ve_lop(tb):
    n = M.resolve_notations(tb)
    assert n["berka:card_type=gold"] == "fin:GoldCard"
    assert n["corebank:k_symbol=SIPO"] == "fin:RecurringPayment"
    assert n["mbd:target_3"] == "fin:AnonymousProduct_3"


def test_uid_on_dinh_va_nhay_theo_tung_truong():
    nk = ids.natural_key(["client_id"], [2])
    assert nk == "client_id=2"
    a = ids.make_uid("fin:Customer", "berka", nk)
    assert a == ids.make_uid("fin:Customer", "berka", nk) and len(a) == 32
    assert a != ids.make_uid("fin:Customer", "corebank", nk)        # hai nguồn = hai uid (+ same_as)
    assert a != ids.make_uid("fin:Person", "berka", nk)
    assert a != ids.make_uid("fin:Customer", "berka", "client_id=3")
    t = ids.make_triple_id(a, "fin:holdsAccount", "x", "1995-01-01", "silver_berka.disp")
    assert t != ids.make_triple_id(a, "fin:holdsAccount", "x", "1995-02-01", "silver_berka.disp")
    assert t != ids.make_triple_id(a, "fin:holdsAccount", "x", "1995-01-01", "silver_corebank.disp")
    assert ids.natural_key(["a", "b"], [None, "x"]) == "a=|b=x"
