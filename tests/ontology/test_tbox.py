"""Hợp đồng ontology (bước O4, docs/ONTOLOGY-BUILD.md): TBox đúng cú pháp, nhất quán, đủ nhãn
cho agent, taxonomy đủ mã nguồn, câu hỏi năng lực có khái niệm, SHACL bắt đồ thị sai, và
tbox.json (Glue đọc) không lệch khỏi TTL."""

from __future__ import annotations

import pathlib
import shutil

import pytest
import yaml

rdflib = pytest.importorskip("rdflib")
from rdflib import OWL, RDF, RDFS, Graph, Namespace, URIRef  # noqa: E402
from rdflib.namespace import SKOS  # noqa: E402

from transforms.onto import tbox as T  # noqa: E402

ROOT = pathlib.Path(__file__).resolve().parents[2]
FIN = Namespace(T.FIN)
FIXTURES = pathlib.Path(__file__).with_name("fixtures")


@pytest.fixture(scope="module")
def g() -> Graph:
    graph = Graph()
    for f in T.TTL_FILES:
        graph.parse(ROOT / f, format="turtle")
    return graph


def _fin_classes(g):
    return sorted(c for c in set(g.subjects(RDF.type, OWL.Class)) if str(c).startswith(T.FIN))


def _fin_props(g):
    out = set()
    for k in (OWL.ObjectProperty, OWL.DatatypeProperty):
        out |= {p for p in g.subjects(RDF.type, k) if str(p).startswith(T.FIN)}
    return sorted(out)


# ---------- nhan va dinh nghia: semantic catalog cua agent sinh tu day ----------


def test_moi_lop_va_quan_he_co_nhan_vi_en_va_dinh_nghia(g):
    missing = []
    for s in _fin_classes(g) + _fin_props(g):
        langs = {getattr(o, "language", None) for o in g.objects(s, RDFS.label)}
        if not {"vi", "en"} <= langs or not list(g.objects(s, SKOS.definition)):
            missing.append(T.curie(str(s)))
    assert not missing, f"thieu nhan vi/en hoac skos:definition: {missing}"


def test_quan_he_doi_tuong_co_khai_mien_hoac_dich(g):
    # ofProduct/recommends tro vao LOP (punning) va frequentlyFollowedBy noi hai lop: khong range
    loose = {"fin:ofProduct", "fin:recommends", "fin:frequentlyFollowedBy", "fin:locatedIn", "fin:derivedFrom"}
    for p in g.subjects(RDF.type, OWL.ObjectProperty):
        if T.curie(str(p)) in loose:
            continue
        assert list(g.objects(p, RDFS.domain)) and list(g.objects(p, RDFS.range)), T.curie(str(p))


# ---------- taxonomy san pham ----------


def _descendants(g, root):
    out, frontier = set(), {root}
    while frontier:
        nxt = {s for x in frontier for s in g.subjects(RDFS.subClassOf, x)} - out
        out |= nxt
        frontier = nxt
    return out


def test_la_taxonomy_san_pham_deu_co_ma_nguon(g):
    products = _descendants(g, FIN.Product)
    leaves = [c for c in products if not list(g.subjects(RDFS.subClassOf, c))]
    no_code = [T.curie(str(c)) for c in leaves if not list(g.objects(c, SKOS.notation))]
    assert len(products) >= 30
    assert not no_code, f"la taxonomy thieu skos:notation: {no_code}"


def test_mot_ma_nguon_chi_thuoc_mot_lop(g):
    seen = {}
    for c, code in g.subject_objects(SKOS.notation):
        if (c, RDF.type, FIN.Typology) in g:
            continue
        assert str(code) not in seen, f"{code} thuoc ca {seen[str(code)]} va {T.curie(str(c))}"
        seen[str(code)] = T.curie(str(c))
    # Hai ma Santander can co de hoc cheo (CQ5, CQ16)
    assert seen["santander:ind_tjcr_fin_ult1"] == "fin:CreditCard"
    assert seen["bank_marketing:y"] == "fin:TermDeposit"


def test_khong_vong_subclass(g):
    for c in _fin_classes(g):
        assert c not in _descendants(g, c), f"vong subClassOf qua {T.curie(str(c))}"


def test_closure_lop_con_tu_json():
    tb = T.load()
    rows = T.subclass_closure(tb)
    anc = {(r["class_iri"], r["ancestor_iri"]) for r in rows}
    assert ("fin:GoldCard", "fin:CreditProduct") in anc and ("fin:GoldCard", "fin:Product") in anc
    assert ("fin:GoldCard", "fin:GoldCard") in anc
    assert ("fin:Loan", "fin:DepositProduct") not in anc


# ---------- cau hoi nang luc (O1) ----------


def test_moi_khai_niem_cua_cau_hoi_nang_luc_co_trong_tbox(g):
    cqs = yaml.safe_load((ROOT / "ontology/competency_questions.yml").read_text(encoding="utf-8"))
    assert len(cqs) >= 20
    known = {T.curie(str(s)) for s in _fin_classes(g) + _fin_props(g)}
    known |= {T.curie(str(s)) for s in g.subjects(RDF.type, FIN.Typology)}
    for q in cqs:
        missing = [c for c in q["concepts"] if c not in known]
        assert not missing, f"{q['id']}: khai niem chua co trong TBox: {missing}"
        assert set(q["purpose"]) <= {"M", "A"}
    # moi CQ ma TBox tro toi deu ton tai trong file
    ids = {q["id"] for q in cqs}
    referenced = {str(o) for o in g.objects(None, FIN.competencyQuestion)}
    assert referenced <= ids, f"TBox tro toi CQ khong co: {referenced - ids}"


# ---------- tbox.json khong lech TTL ----------


def test_tbox_json_khop_ttl():
    assert T.JSON_PATH.read_text(encoding="utf-8") == T.dumps(T.compile_tbox()), \
        "tbox.json lech TTL: chay `python -m transforms.onto.tbox`"


# ---------- SHACL (O4) ----------


def _validate(data_ttl: str):
    pyshacl = pytest.importorskip("pyshacl")
    data = Graph().parse(data=data_ttl, format="turtle")
    # Gop TBox vao do thi du lieu (pyshacl khong tron ont_graph khi tat suy dien): sh:class can
    # thay kieu cua ca the TBox (fin:FanOut a fin:Typology) va chuoi subClassOf.
    for f in T.TTL_FILES:
        data.parse(ROOT / f, format="turtle")
    shapes = Graph().parse(ROOT / "ontology/shapes/fingraph.shacl.ttl", format="turtle")
    # inference="none": suy dien RDFS se GAN kieu theo rdfs:range (holdsAccount -> District thanh
    # Account) va che mat dung loi can bat. Job build_graph (O7) cung kiem lop TUONG MINH.
    conforms, _, text = pyshacl.validate(data, shacl_graph=shapes, inference="none", allow_warnings=True)
    return conforms, text


def test_shacl_do_thi_mau_hop_le():
    conforms, text = _validate((FIXTURES / "sample_abox.ttl").read_text(encoding="utf-8"))
    assert conforms, text


PREFIX = """@prefix fin: <https://fingraph.hust.edu.vn/ontology#> .
@prefix fid: <https://fingraph.hust.edu.vn/id/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
"""


@pytest.mark.parametrize("bad,why", [
    ("fid:h a fin:ProductHolding ; fin:validFrom '2020-01-01'^^xsd:date .", "ofProduct"),
    ("fid:h a fin:ProductHolding ; fin:ofProduct fin:Loan ; fin:validFrom '2020-02-01'^^xsd:date ;"
     " fin:validTo '2020-01-01'^^xsd:date .", "validTo < validFrom"),
    ("fid:h a fin:ProductHolding ; fin:ofProduct fin:District ; fin:validFrom '2020-01-01'^^xsd:date .",
     "cay fin:Product"),
    ("fid:c a fin:Customer . fid:d a fin:District . fid:c fin:holdsAccount fid:d .", "Account"),
    ("fid:x a fin:LaunderingAttempt .", "kieu hinh"),
])
def test_shacl_bat_do_thi_sai(bad, why):
    conforms, text = _validate(PREFIX + bad)
    assert not conforms and why in text, text


# ---------- OWL 2 DL nhat quan (HermiT, can Java) ----------


@pytest.mark.skipif(shutil.which("java") is None, reason="can Java cho HermiT")
def test_reasoner_tbox_nhat_quan(g, tmp_path):
    owlready2 = pytest.importorskip("owlready2")
    nt = tmp_path / "fingraph.nt"
    g.serialize(destination=str(nt), format="nt")
    world = owlready2.World()
    with open(nt, "rb") as f:
        onto = world.get_ontology("https://fingraph.hust.edu.vn/ontology").load(fileobj=f, format="ntriples")
    # xsd:date khong thuoc OWL 2 datatype map (chi xsd:dateTime). Giu xsd:date vi cot lakehouse la
    # NGAY; reasoner bo qua kieu khong ho tro, van kiem do nhat quan cua lop va quan he.
    with onto:
        owlready2.sync_reasoner_hermit(world, infer_property_values=False, debug=0,
                                       ignore_unsupported_datatypes=True)
    inconsistent = list(world.inconsistent_classes())
    assert not inconsistent, f"lop mau thuan: {inconsistent}"
