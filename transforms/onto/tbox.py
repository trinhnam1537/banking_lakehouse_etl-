"""Biên dịch TBox Turtle -> tbox.json (bước O3/O7, docs/ONTOLOGY-BUILD.md).

    python -m transforms.onto.tbox            # ghi lại transforms/onto/tbox.json
    python -m transforms.onto.tbox --check    # khác file đang có thì exit 1 (CI)

Vì sao có bước biên dịch: nguồn sự thật là `ontology/fingraph.ttl` (người duyệt đọc,
reasoner kiểm), nhưng Glue không có rdflib. JSON đóng gói trong etl.zip, job
`onto.load_schema` và semantic catalog của agent đọc JSON này. pytest kiểm JSON không
lệch khỏi TTL, nên không bao giờ có hai bản TBox khác nhau.

Hàm `load()` chỉ dùng thư viện chuẩn (chạy được trên Glue); `compile_tbox()` cần rdflib.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
from typing import Any, Dict, List

REPO = pathlib.Path(__file__).resolve().parents[2]
TTL_FILES = ("ontology/fingraph.ttl", "ontology/alignment/fibo.ttl")
JSON_PATH = pathlib.Path(__file__).with_name("tbox.json")

FIN = "https://fingraph.hust.edu.vn/ontology#"
PREFIXES = {
    FIN: "fin:",
    "http://www.w3.org/2004/02/skos/core#": "skos:",
    "http://www.w3.org/2001/XMLSchema#": "xsd:",
    "http://www.w3.org/2002/07/owl#": "owl:",
}


def curie(iri: str) -> str:
    for base, p in PREFIXES.items():
        if iri.startswith(base):
            return p + iri[len(base):]
    return iri


def compile_tbox(root: pathlib.Path = REPO) -> Dict[str, Any]:
    from rdflib import OWL, RDF, RDFS, Graph, URIRef
    from rdflib.namespace import SKOS

    g = Graph()
    for f in TTL_FILES:
        g.parse(root / f, format="turtle")
    fin = lambda s: URIRef(FIN + s)  # noqa: E731

    def lit(s, p, lang=None) -> List[str]:
        vals = [o for o in g.objects(s, p)]
        if lang:
            vals = [o for o in vals if getattr(o, "language", None) == lang]
        return sorted(str(v) for v in vals)

    def one(s, p, lang=None) -> str:
        v = lit(s, p, lang)
        return v[0] if v else ""

    classes = []
    for c in sorted(set(g.subjects(RDF.type, OWL.Class))):
        if not str(c).startswith(FIN):
            continue
        parents = sorted(curie(str(o)) for o in g.objects(c, RDFS.subClassOf))
        classes.append({
            "iri": curie(str(c)), "label_vi": one(c, RDFS.label, "vi"), "label_en": one(c, RDFS.label, "en"),
            "definition": one(c, SKOS.definition, "vi"), "parents": parents,
            "notations": lit(c, SKOS.notation), "competency_questions": lit(c, fin("competencyQuestion")),
            "source_datasets": lit(c, fin("sourceDataset")), "materialized_in": lit(c, fin("materializedIn")),
            "close_match": sorted(str(o) for o in g.objects(c, SKOS.closeMatch)),
        })

    props = []
    kinds = {OWL.ObjectProperty: "object", OWL.DatatypeProperty: "datatype"}
    for kind_iri, kind in kinds.items():
        for p in sorted(set(g.subjects(RDF.type, kind_iri))):
            types = set(g.objects(p, RDF.type))
            props.append({
                "iri": curie(str(p)), "kind": kind,
                "label_vi": one(p, RDFS.label, "vi"), "label_en": one(p, RDFS.label, "en"),
                "definition": one(p, SKOS.definition, "vi"),
                "domain": one(p, RDFS.domain) and curie(one(p, RDFS.domain)),
                "range": one(p, RDFS.range) and curie(one(p, RDFS.range)),
                "transitive": OWL.TransitiveProperty in types, "symmetric": OWL.SymmetricProperty in types,
                "competency_questions": lit(p, fin("competencyQuestion")),
            })
    props.sort(key=lambda x: x["iri"])

    concepts = []
    for ind in sorted(set(g.subjects(RDF.type, fin("Typology")))):
        concepts.append({"iri": curie(str(ind)), "scheme": "fin:typologyScheme",
                         "notation": one(ind, SKOS.notation), "pref_label": one(ind, SKOS.prefLabel, "en")})

    version = one(URIRef("https://fingraph.hust.edu.vn/ontology"), OWL.versionInfo)
    return {"version": version, "classes": classes, "properties": props, "concepts": concepts}


def subclass_closure(tbox: Dict[str, Any]) -> List[Dict[str, Any]]:
    """(lớp, tổ tiên, khoảng cách), kể cả chính nó với khoảng cách 0. Chỉ thư viện chuẩn."""
    parents = {c["iri"]: [p for p in c["parents"] if p.startswith("fin:")] for c in tbox["classes"]}
    out = []
    for c in parents:
        seen = {c: 0}
        frontier = [c]
        while frontier:
            nxt = []
            for x in frontier:
                for p in parents.get(x, []):
                    if p not in seen:
                        seen[p] = seen[x] + 1
                        nxt.append(p)
            frontier = nxt
        out.extend({"class_iri": c, "ancestor_iri": a, "depth": d} for a, d in sorted(seen.items()))
    return out


def load(path: pathlib.Path = JSON_PATH) -> Dict[str, Any]:
    """Đọc tbox.json: từ thư mục hoặc từ trong etl.zip (Glue)."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, NotADirectoryError):
        import importlib.resources as ir

        return json.loads(ir.files("transforms.onto").joinpath("tbox.json").read_text(encoding="utf-8"))


def dumps(tbox: Dict[str, Any]) -> str:
    return json.dumps(tbox, ensure_ascii=False, indent=1, sort_keys=True) + "\n"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--check", action="store_true", help="khong ghi; lech thi exit 1")
    args = ap.parse_args(argv)
    text = dumps(compile_tbox())
    if args.check:
        same = JSON_PATH.exists() and JSON_PATH.read_text(encoding="utf-8") == text
        print("tbox.json khop TTL" if same else "tbox.json LECH TTL: chay python -m transforms.onto.tbox")
        return 0 if same else 1
    JSON_PATH.write_text(text, encoding="utf-8")
    t = json.loads(text)
    print(f"ghi {JSON_PATH}: {len(t['classes'])} lop, {len(t['properties'])} quan he, {len(t['concepts'])} khai niem")
    return 0


if __name__ == "__main__":
    sys.exit(main())
