"""Mapping nguồn -> ontology (bước O5, docs/ONTOLOGY-BUILD.md): đọc và KIỂM TRA TĨNH.

Mỗi nguồn một file `transforms/onto/mappings/<nguồn>.yml` — nguồn sự thật, người duyệt sửa.
Glue Spark không có PyYAML, nên YAML được biên dịch thành `mappings.json` (giống tbox.json):

    python -m transforms.onto.mapping            # ghi lại mappings.json
    python -m transforms.onto.mapping --check    # CI: JSON lệch YAML thì exit 1

Ba loại mục:

    entities   một node mỗi khoá:        class, key, label, valid_from/to, attributes
    edges      một cạnh mỗi dòng nguồn:  predicate, subject, object (node) | object_literal
    holdings   sự kiện "khách giữ sản phẩm" (n-ary): sinh node fin:ProductHolding + các cạnh
               hasHolding, ofProduct, viaAccount, holdingStatus, amount

Nguồn của mỗi mục là `from: <bảng>` (+ `where`) trong `database` của file, hoặc `sql:` tự viết
khi cần join/gộp. Trong sql, `{silver_x.bang}` là bảng silver ĐÃ LỌC bản hiện hành (bỏ dòng xoá,
lấy phiên bản SCD2 hiện hành) — job thay bằng view tạm.

Kiểm tĩnh (không cần Spark, chạy trong pytest và đầu job): lớp/quan hệ có trong TBox, tham
chiếu node tồn tại, số cột khoá khớp, và KIỂU của hai đầu cạnh khớp domain/range theo bao đóng
subClassOf. Kiểm kiểu tường minh ở đây vì suy diễn RDFS sẽ "chữa" cạnh sai thay vì báo lỗi
(phát hiện ở bước O4, xem ONTOLOGY-BUILD.md).
"""

from __future__ import annotations

import json
import pathlib
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Set

MAPPING_DIR = pathlib.Path(__file__).with_name("mappings")
JSON_PATH = pathlib.Path(__file__).with_name("mappings.json")  # bản biên dịch cho Glue
TABLE_REF = re.compile(r"\{([A-Za-z_][\w]*)\.([A-Za-z_][\w]*)\}")
DATATYPES = {"xsd:string", "xsd:integer", "xsd:decimal", "xsd:double", "xsd:date", "xsd:boolean"}


class MappingError(ValueError):
    pass


@dataclass
class Source:
    """Nguồn của một mục: SQL đã chuẩn hoá về dạng có `{db.bang}`."""
    sql: str
    tables: List[str]

    @property
    def dataset(self) -> str:
        return ",".join(self.tables)


@dataclass
class Ref:
    entity: str              # id của mục entities trong cùng file
    key: List[str]           # biểu thức SQL, khớp theo vị trí với key của node


@dataclass
class Entity:
    id: str
    cls: str
    also: List[str]
    source: Source
    key: Dict[str, str]      # tên -> biểu thức, có thứ tự
    label: str
    valid_from: Optional[str]
    valid_to: Optional[str]
    attributes: Dict[str, str]


@dataclass
class Edge:
    id: str
    predicate: str
    source: Source
    subject: Ref
    obj: Optional[Ref]
    literal: Optional[Dict[str, str]]   # {expr, datatype}
    valid_from: Optional[str]
    valid_to: Optional[str]
    weight: Optional[str]
    source_key: Optional[str]
    expect_rows: Optional[str]


@dataclass
class Holding:
    id: str
    product: Optional[str]              # lớp sản phẩm cố định
    product_notation: Optional[str]     # hoặc biểu thức -> mã nguồn, tra skos:notation
    source: Source
    key: Dict[str, str]
    customer: Ref
    account: Optional[Ref]
    valid_from: Optional[str]
    valid_to: Optional[str]
    status: Optional[str]
    amount: Optional[str]
    source_key: Optional[str]
    expect_rows: Optional[str]
    attributes: Dict[str, str] = field(default_factory=dict)


@dataclass
class Mapping:
    name: str
    source_system: str
    database: str
    reviewed_by: str
    entities: Dict[str, Entity] = field(default_factory=dict)
    edges: List[Edge] = field(default_factory=list)
    holdings: List[Holding] = field(default_factory=list)

    def tables(self) -> Set[str]:
        out: Set[str] = set()
        for item in [*self.entities.values(), *self.edges, *self.holdings]:
            out.update(item.source.tables)
        return out


# ---------------------------------------------------------------- đọc

def _source(raw: Dict[str, Any], database: str, where_ctx: str) -> Source:
    if ("from" in raw) == ("sql" in raw):
        raise MappingError(f"{where_ctx}: can dung MOT trong `from` hoac `sql`")
    if "from" in raw:
        tbl = raw["from"] if "." in raw["from"] else f"{database}.{raw['from']}"
        sql = f"SELECT * FROM {{{tbl}}}"
        if raw.get("where"):
            sql += f" WHERE {raw['where']}"
    else:
        if raw.get("where"):
            raise MappingError(f"{where_ctx}: `where` chi dung voi `from`; voi `sql` viet WHERE trong cau")
        sql = raw["sql"]
    tables = sorted({f"{a}.{b}" for a, b in TABLE_REF.findall(sql)})
    if not tables:
        raise MappingError(f"{where_ctx}: sql khong tham chieu bang silver nao dang {{db.bang}}")
    return Source(sql=sql.strip(), tables=tables)


def _ref(raw: Any, ctx: str) -> Ref:
    if not isinstance(raw, dict) or "entity" not in raw or "key" not in raw:
        raise MappingError(f"{ctx}: tham chieu node can {{entity, key}}")
    key = raw["key"] if isinstance(raw["key"], list) else [raw["key"]]
    return Ref(entity=raw["entity"], key=[str(k) for k in key])


def _key(raw: Any, ctx: str) -> Dict[str, str]:
    if isinstance(raw, list):           # [client_id] -> {client_id: client_id}
        raw = {k: k for k in raw}
    if not isinstance(raw, dict) or not raw:
        raise MappingError(f"{ctx}: `key` rong")
    return {str(k): str(v) for k, v in raw.items()}


def parse(raw: Dict[str, Any], name: str) -> Mapping:
    for f in ("source_system", "database", "reviewed_by"):
        if not raw.get(f):
            raise MappingError(f"{name}: thieu `{f}`")
    m = Mapping(name=name, source_system=raw["source_system"], database=raw["database"],
                reviewed_by=raw["reviewed_by"])
    db = m.database
    for e in raw.get("entities") or []:
        ctx = f"{name}.entities.{e.get('id')}"
        if e["id"] in m.entities:
            raise MappingError(f"{ctx}: id trung")
        m.entities[e["id"]] = Entity(
            id=e["id"], cls=e["class"], also=list(e.get("also") or []), source=_source(e, db, ctx),
            key=_key(e.get("key"), ctx), label=e["label"], valid_from=e.get("valid_from"),
            valid_to=e.get("valid_to"), attributes={str(k): str(v) for k, v in (e.get("attributes") or {}).items()})
    for e in raw.get("edges") or []:
        ctx = f"{name}.edges.{e.get('id')}"
        if ("object" in e) == ("object_literal" in e):
            raise MappingError(f"{ctx}: can MOT trong `object` hoac `object_literal`")
        m.edges.append(Edge(
            id=e["id"], predicate=e["predicate"], source=_source(e, db, ctx), subject=_ref(e.get("subject"), ctx),
            obj=_ref(e["object"], ctx) if "object" in e else None, literal=e.get("object_literal"),
            valid_from=e.get("valid_from"), valid_to=e.get("valid_to"), weight=e.get("weight"),
            source_key=e.get("source_key"), expect_rows=e.get("expect_rows")))
    for h in raw.get("holdings") or []:
        ctx = f"{name}.holdings.{h.get('id')}"
        if ("product" in h) == ("product_notation" in h):
            raise MappingError(f"{ctx}: can MOT trong `product` hoac `product_notation`")
        m.holdings.append(Holding(
            id=h["id"], product=h.get("product"), product_notation=h.get("product_notation"),
            source=_source(h, db, ctx), key=_key(h.get("key"), ctx), customer=_ref(h.get("customer"), ctx),
            account=_ref(h["account"], ctx) if h.get("account") else None, valid_from=h.get("valid_from"),
            valid_to=h.get("valid_to"), status=h.get("status"), amount=h.get("amount"),
            source_key=h.get("source_key"), expect_rows=h.get("expect_rows"),
            attributes={str(k): str(v) for k, v in (h.get("attributes") or {}).items()}))
    ids = [x.id for x in [*m.edges, *m.holdings]] + list(m.entities)
    dup = {i for i in ids if ids.count(i) > 1}
    if dup:
        raise MappingError(f"{name}: id trung giua cac muc: {sorted(dup)}")
    return m


def read_yaml_dir(directory: pathlib.Path = MAPPING_DIR) -> Dict[str, Dict[str, Any]]:
    """YAML gốc (người duyệt sửa) -> dict theo tên file. Cần PyYAML: chỉ chạy trên máy/CI."""
    import yaml

    out = {}
    for p in sorted(directory.glob("*.yml")):
        with open(p, encoding="utf-8") as f:
            out[p.stem] = yaml.safe_load(f) or {}
    return out


def dumps(raw: Dict[str, Dict[str, Any]]) -> str:
    return json.dumps(raw, ensure_ascii=False, indent=1, sort_keys=True) + "\n"


def load_raw() -> Dict[str, Dict[str, Any]]:
    """mappings.json đóng gói trong etl.zip (Glue Spark không có PyYAML). Chỉ thư viện chuẩn."""
    if JSON_PATH.is_file():
        return json.loads(JSON_PATH.read_text(encoding="utf-8"))
    import importlib.resources as ir

    return json.loads(ir.files("transforms.onto").joinpath("mappings.json").read_text(encoding="utf-8"))


def load_all(names: Optional[Iterable[str]] = None) -> List[Mapping]:
    wanted = set(names) if names else None
    raw = load_raw()
    out = [parse(raw[k], k) for k in sorted(raw) if wanted is None or k in wanted]
    if wanted and {m.name for m in out} != wanted:
        raise MappingError(f"Khong co mapping: {sorted(wanted - {m.name for m in out})}")
    return out


def main(argv=None) -> int:
    """python -m transforms.onto.mapping [--check]: YAML -> mappings.json (như tbox.json)."""
    import argparse
    import sys

    ap = argparse.ArgumentParser(description="Bien dich mapping YAML -> mappings.json")
    ap.add_argument("--check", action="store_true", help="khac file dang co thi exit 1 (CI)")
    a = ap.parse_args(argv)
    raw = read_yaml_dir()
    for k, v in raw.items():
        parse(v, k)
    text = dumps(raw)
    if a.check:
        same = JSON_PATH.is_file() and JSON_PATH.read_text(encoding="utf-8") == text
        print("mappings.json khop YAML" if same else "mappings.json LECH YAML: chay python -m transforms.onto.mapping")
        return 0 if same else 1
    JSON_PATH.write_text(text, encoding="utf-8", newline="\n")
    print(f"ghi {JSON_PATH} ({len(raw)} mapping)")
    return 0


# ---------------------------------------------------------------- kiểm tĩnh

def ancestors(tbox: Dict[str, Any]) -> Dict[str, Set[str]]:
    from .tbox import subclass_closure

    anc: Dict[str, Set[str]] = {}
    for r in subclass_closure(tbox):
        anc.setdefault(r["class_iri"], set()).add(r["ancestor_iri"])
    return anc


def resolve_notations(tbox: Dict[str, Any]) -> Dict[str, str]:
    """'berka:card_type=gold' -> 'fin:GoldCard'. Một mã chỉ được trỏ một lớp."""
    out: Dict[str, str] = {}
    for c in tbox["classes"]:
        for n in c["notations"]:
            if n in out and out[n] != c["iri"]:
                raise MappingError(f"Ma {n} tro hai lop {out[n]} va {c['iri']}")
            out[n] = c["iri"]
    return out


def validate(mappings: List[Mapping], tbox: Dict[str, Any]) -> None:
    classes = {c["iri"] for c in tbox["classes"]}
    props = {p["iri"]: p for p in tbox["properties"]}
    anc = ancestors(tbox)
    is_a = lambda types, target: any(target in anc.get(t, {t}) for t in types)  # noqa: E731
    errors: List[str] = []

    def need_prop(iri: str, ctx: str, kind: str) -> Optional[Dict[str, Any]]:
        p = props.get(iri)
        if p is None:
            errors.append(f"{ctx}: quan he {iri} khong co trong TBox")
        elif p["kind"] != kind:
            errors.append(f"{ctx}: {iri} la {p['kind']}, mapping dung nhu {kind}")
        return p

    def check_ends(p: Dict[str, Any], subj_types: List[str], obj_types: Optional[List[str]], ctx: str):
        if p.get("domain") and not is_a(subj_types, p["domain"]):
            errors.append(f"{ctx}: chu the {subj_types} khong phai {p['domain']} (domain {p['iri']})")
        if obj_types is not None and p.get("range") and not is_a(obj_types, p["range"]):
            errors.append(f"{ctx}: doi tuong {obj_types} khong phai {p['range']} (range {p['iri']})")

    def ref_types(m: Mapping, r: Ref, ctx: str) -> Optional[List[str]]:
        e = m.entities.get(r.entity)
        if e is None:
            errors.append(f"{ctx}: khong co node `{r.entity}` trong {m.name}")
            return None
        if len(r.key) != len(e.key):
            errors.append(f"{ctx}: `{r.entity}` co {len(e.key)} cot khoa, tham chieu dua {len(r.key)}")
        return [e.cls, *e.also]

    names = [m.name for m in mappings]
    systems = [m.source_system for m in mappings]
    if len(set(systems)) != len(systems):
        errors.append(f"source_system trung giua cac file: {names}")
    notations = resolve_notations(tbox)
    for m in mappings:
        for e in m.entities.values():
            for c in [e.cls, *e.also]:
                if c not in classes:
                    errors.append(f"{m.name}.{e.id}: lop {c} khong co trong TBox")
        for e in m.edges:
            ctx = f"{m.name}.{e.id}"
            if e.expect_rows and not e.source_key:
                # Đối soát đếm source_key KHÁC NHAU: khoá mặc định (khoá chủ thể) lặp giữa các dòng
                errors.append(f"{ctx}: co expect_rows thi phai khai source_key duy nhat moi dong nguon")
            st = ref_types(m, e.subject, ctx)
            if e.obj is not None:
                p = need_prop(e.predicate, ctx, "object")
                ot = ref_types(m, e.obj, ctx)
                if p and st is not None and ot is not None:
                    check_ends(p, st, ot, ctx)
            else:
                p = need_prop(e.predicate, ctx, "datatype")
                lit = e.literal or {}
                if "expr" not in lit or lit.get("datatype") not in DATATYPES:
                    errors.append(f"{ctx}: object_literal can expr va datatype thuoc {sorted(DATATYPES)}")
                if p and st is not None:
                    check_ends(p, st, None, ctx)
        for h in m.holdings:
            ctx = f"{m.name}.{h.id}"
            ct = ref_types(m, h.customer, ctx)
            if ct is not None and not is_a(ct, "fin:Customer"):
                errors.append(f"{ctx}: customer {ct} khong phai fin:Customer")
            if h.account is not None:
                at = ref_types(m, h.account, ctx)
                if at is not None and not is_a(at, "fin:Account"):
                    errors.append(f"{ctx}: account {at} khong phai fin:Account")
            if h.product is not None:
                if h.product not in classes or not is_a([h.product], "fin:Product"):
                    errors.append(f"{ctx}: {h.product} khong phai lop con cua fin:Product")
            elif not any(n.startswith(m.source_system + ":") for n in notations):
                errors.append(f"{ctx}: TBox khong co skos:notation nao cho nguon {m.source_system}")
    if errors:
        raise MappingError("Mapping sai:\n  - " + "\n  - ".join(errors))


if __name__ == "__main__":
    raise SystemExit(main())
