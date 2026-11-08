"""Semantic catalog (bước O11): bản đồ nghĩa cho agent, SINH TỰ ĐỘNG từ TBox + mapping.

    python -m agent.catalog          # ghi agent/catalog/catalog.json + catalog.md
    python -m agent.catalog --check  # CI: lệch TBox/mapping thì exit 1

Vì sao sinh tự động: catalog viết tay sẽ lệch khỏi ontology sau vài lần sửa TBox, và agent sẽ
tự tin trả lời theo một bản đồ cũ. Sinh từ tbox.json + mappings.json thì sửa ontology là catalog
đổi theo; test chặn bản commit bị cũ.

Hai dạng:
  - prompt_text(): bản RÚT GỌN cho system prompt (mỗi lớp/quan hệ một dòng). Ổn định từng byte
    giữa các lần gọi để prompt caching hoạt động — không có ngày giờ, thứ tự cố định.
  - describe(term): bản CHI TIẾT một khái niệm, phục vụ tool `describe`.
Catalog không chứa dữ liệu khách nào (checklist O11).
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
import unicodedata
from typing import Any, Dict, List, Optional

from transforms.onto import mapping as M
from transforms.onto import tbox as T

OUT_DIR = pathlib.Path(__file__).with_name("catalog")


def build(tb: Optional[Dict[str, Any]] = None, mappings: Optional[List[M.Mapping]] = None) -> Dict[str, Any]:
    tb = tb or T.load()
    mappings = mappings if mappings is not None else M.load_all()
    children: Dict[str, List[str]] = {}
    for c in tb["classes"]:
        for p in c["parents"]:
            children.setdefault(p, []).append(c["iri"])
    # Lớp nào có dữ liệu từ nguồn nào (theo mapping), để agent biết hỏi được gì và không hỏi được gì
    sources: Dict[str, set] = {}
    for m in mappings:
        for e in m.entities.values():
            for cls in [e.cls, *e.also]:
                sources.setdefault(cls, set()).add(m.source_system)
        for h in m.holdings:
            sources.setdefault("fin:ProductHolding", set()).add(m.source_system)
            if h.product:
                sources.setdefault(h.product, set()).add(m.source_system)
    notations = M.resolve_notations(tb)
    for code, cls in notations.items():
        src = code.split(":", 1)[0]
        if any(m.source_system == src for m in mappings):
            sources.setdefault(cls, set()).add(src)
    # Lớp cha "có dữ liệu" khi một lớp con có: "thẻ tín dụng" có dữ liệu nhờ thẻ classic/gold.
    # Không lan thì agent đọc catalog sẽ trả lời "không có dữ liệu thẻ tín dụng" — sai.
    for cls, anc in M.ancestors(tb).items():
        for a in anc - {cls}:
            if sources.get(cls):
                sources.setdefault(a, set()).update(sources[cls])
    preds: Dict[str, set] = {}
    for m in mappings:
        for e in m.edges:
            preds.setdefault(e.predicate, set()).add(m.source_system)
            if e.obj is not None and e.obj.concept is not None:   # node khai niem (kieu hinh AML)
                for cls in M.SCHEME_CLASS.values():
                    sources.setdefault(cls, set()).add(m.source_system)
        for h in m.holdings:
            for p in ("fin:hasHolding", "fin:ofProduct"):
                preds.setdefault(p, set()).add(m.source_system)
            if h.account:
                preds.setdefault("fin:viaAccount", set()).add(m.source_system)
            if h.status:
                preds.setdefault("fin:holdingStatus", set()).add(m.source_system)
            if h.amount:
                preds.setdefault("fin:amount", set()).add(m.source_system)
    # Canh fin:sameAs sinh tu luat O8 (transforms/onto/resolve.py), khong tu mapping
    from transforms.onto import resolve as R

    present = {m.source_system for m in mappings}
    for r in R.RULES:
        if {r["a"]["source_system"], r["b"]["source_system"]} <= present:
            preds.setdefault("fin:sameAs", set()).update({r["a"]["source_system"], r["b"]["source_system"]})
    classes = [{
        "iri": c["iri"], "label_vi": c["label_vi"], "label_en": c["label_en"], "definition": c["definition"],
        "parents": c["parents"], "children": sorted(children.get(c["iri"], [])), "notations": c["notations"],
        "graph_sources": sorted(sources.get(c["iri"], [])), "materialized_in": c["materialized_in"],
    } for c in sorted(tb["classes"], key=lambda c: c["iri"])]
    props = [{
        "iri": p["iri"], "kind": p["kind"], "label_vi": p["label_vi"], "definition": p["definition"],
        "domain": p["domain"], "range": p["range"], "transitive": p["transitive"],
        "graph_sources": sorted(preds.get(p["iri"], [])),
    } for p in sorted(tb["properties"], key=lambda p: p["iri"])]
    return {"ontology_version": tb["version"], "classes": classes, "properties": props,
            "sources": sorted(m.source_system for m in mappings)}


def prompt_text(cat: Dict[str, Any]) -> str:
    """Bản rút gọn cho system prompt. Ổn định từng byte (prompt caching)."""
    lines = [f"# Ontology FinGraph {cat['ontology_version']} (namespace fin:)",
             f"Nguon da len do thi: {', '.join(cat['sources'])}.",
             "Lop | nhan | cha | nguon co du lieu:"]
    for c in cat["classes"]:
        parent = ",".join(c["parents"]) or "-"
        src = ",".join(c["graph_sources"]) or "(chua co du lieu)"
        lines.append(f"- {c['iri']} | {c['label_vi']} / {c['label_en']} | {parent} | {src}")
    lines.append("Quan he | nhan | domain -> range | nguon:")
    for p in cat["properties"]:
        rng = p["range"] or ("literal" if p["kind"] == "datatype" else "-")
        src = ",".join(p["graph_sources"]) or "(chua co du lieu)"
        lines.append(f"- {p['iri']} | {p['label_vi']} | {p['domain'] or '-'} -> {rng} | {src}")
    return "\n".join(lines) + "\n"


def _fold(s: str) -> str:
    """Chữ thường, bỏ dấu tiếng Việt: 'thẻ tín dụng' và 'the tin dung' tra ra cùng lớp."""
    s = unicodedata.normalize("NFKD", s.replace("đ", "d").replace("Đ", "D"))
    return "".join(ch for ch in s if not unicodedata.combining(ch)).lower().strip()


def describe(cat: Dict[str, Any], term: str) -> List[Dict[str, Any]]:
    """Tra một khái niệm theo IRI, nhãn vi/en (không phân biệt hoa thường, có dấu hay không) hoặc mã nguồn."""
    t = _fold(term)
    hits = []
    for c in cat["classes"]:
        labels = (_fold(c["label_vi"]), _fold(c["label_en"]))
        keys = {_fold(c["iri"]), *labels, *[_fold(n) for n in c["notations"]]}
        if t in keys or (len(t) >= 3 and any(t in k for k in labels)):
            hits.append({"type": "class", **c})
    for p in cat["properties"]:
        if t in {_fold(p["iri"]), _fold(p["label_vi"])}:
            hits.append({"type": "property", **p})
    return hits[:10]


def dumps(cat: Dict[str, Any]) -> str:
    return json.dumps(cat, ensure_ascii=False, indent=1, sort_keys=True) + "\n"


def load(path: pathlib.Path = OUT_DIR / "catalog.json") -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Sinh semantic catalog cho agent")
    ap.add_argument("--check", action="store_true")
    a = ap.parse_args(argv)
    cat = build()
    files = {OUT_DIR / "catalog.json": dumps(cat), OUT_DIR / "catalog.md": prompt_text(cat)}
    if a.check:
        bad = [str(p) for p, txt in files.items() if not p.is_file() or p.read_text(encoding="utf-8") != txt]
        print("catalog khop TBox/mapping" if not bad else f"catalog CU: {bad} — chay python -m agent.catalog")
        return 1 if bad else 0
    OUT_DIR.mkdir(exist_ok=True)
    for p, txt in files.items():
        p.write_text(txt, encoding="utf-8", newline="\n")
        print(f"ghi {p}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
