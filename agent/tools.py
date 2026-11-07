"""Tool của agent (bước O11): HẸP, CÓ KIỂU, mỗi kết quả kèm uid / triple_id để trích dẫn.

Vì sao tool hẹp thay vì để LLM tự viết SQL: text-to-SQL trên schema thô ra câu đúng cú pháp nhưng
sai nghĩa (không biết đi disp -> card, không biết "thẻ tín dụng" gồm mọi lớp con). Mỗi tool ở đây
mã hoá sẵn một đường đi đúng trên đồ thị; LLM chỉ chọn tool và điền tham số.

Mọi tham số đi vào SQL đều được KIỂM bằng regex hoặc escape trước (uid 32 hex, lớp `fin:Xxx`,
ngày ISO): chặn SQL injection từ prompt. Tool `query` chỉ cho SELECT/WITH trên database cho phép,
và dù vậy vẫn chạy dưới workgroup Athena của persona (trần quét) + quyền Lake Formation của persona:
lớp kiểm ở đây là lớp thứ nhất, không phải lớp duy nhất.

Backend tách riêng (run_sql) để test không cần AWS và để sau này đổi sang Neptune (openCypher)
cho truy vấn nhiều bước nhảy mà không đổi hợp đồng tool (docs/NEPTUNE.md).
"""

from __future__ import annotations

import re
import time
from typing import Any, Callable, Dict, List, Optional, Protocol

from . import catalog as C

DB = "track_a_onto"
UID = re.compile(r"^[0-9a-f]{32}$")
CLASS = re.compile(r"^fin:[A-Za-z][A-Za-z0-9_]*$")
DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
READ_DBS = ("track_a_onto", "track_a_gold")
FORBIDDEN = re.compile(r"\b(insert|update|delete|merge|drop|create|alter|grant|revoke|unload|msck|call|optimize|vacuum)\b",
                       re.IGNORECASE)
MAX_ROWS = 200


class ToolError(ValueError):
    """Lỗi do tham số: trả về cho LLM dưới dạng tool_result is_error để nó tự sửa."""


class Backend(Protocol):
    def run_sql(self, sql: str) -> List[Dict[str, Any]]: ...


class GraphBackend(Protocol):
    """openCypher CHỈ ĐỌC trên Neptune (bản chiếu track_a_onto, xem docs/NEPTUNE.md)."""
    def run_cypher(self, query: str, params: Dict[str, Any]) -> List[Dict[str, Any]]: ...


def _uid(v: str) -> str:
    if not isinstance(v, str) or not UID.match(v):
        raise ToolError(f"uid phai la 32 ky tu hex, nhan duoc {v!r}. Dung find_entity de lay uid.")
    return v


def _cls(v: str) -> str:
    if not isinstance(v, str) or not CLASS.match(v):
        raise ToolError(f"class_iri phai dang fin:TenLop, nhan duoc {v!r}. Dung describe de tra lop.")
    return v


def _date(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    if not isinstance(v, str) or not DATE.match(v):
        raise ToolError(f"ngay phai dang YYYY-MM-DD, nhan duoc {v!r}")
    return v


def _text(v: str, n: int = 80) -> str:
    if not isinstance(v, str) or not v.strip():
        raise ToolError("text rong")
    return v.strip()[:n].replace("'", "''")


def _at(alias: str, as_of: Optional[str]) -> str:
    if not as_of:
        return ""
    return f" AND {alias}.valid_from <= DATE '{as_of}' AND ({alias}.valid_to IS NULL OR {alias}.valid_to >= DATE '{as_of}')"


# ---------------------------------------------------------------- SQL (tách để test)

def sql_find_entity(text: str, class_iri: Optional[str], limit: int) -> str:
    t = _text(text).lower()
    where = f"lower(label) LIKE '%{t}%'"
    if class_iri:
        where += f" AND (class_iri = '{_cls(class_iri)}' OR contains(also_types, '{class_iri}'))"
    return (f"SELECT uid, class_iri, label, source_system FROM {DB}.entity WHERE {where} "
            f"ORDER BY label LIMIT {max(1, min(int(limit), 50))}")


def sql_customer_360(uid: str, as_of: Optional[str]) -> str:
    u, d = _uid(uid), _date(as_of)
    return f"""
SELECT 'holding' AS kind, h.triple_id, p.class_iri AS item_class, p.label AS item_label, hd.uid AS item_uid,
       h.valid_from, h.valid_to, element_at(hd.attributes, 'status') AS detail, h.source_dataset
FROM {DB}.triple h
JOIN {DB}.entity hd ON hd.uid = h.object_uid
JOIN {DB}.triple o ON o.subject_uid = h.object_uid AND o.predicate_iri = 'fin:ofProduct'
JOIN {DB}.entity p ON p.uid = o.object_uid
WHERE h.subject_uid = '{u}' AND h.predicate_iri = 'fin:hasHolding'{_at('h', d)}
UNION ALL
SELECT replace(t.predicate_iri, 'fin:', '') AS kind, t.triple_id, e.class_iri, e.label, e.uid,
       t.valid_from, t.valid_to, CAST(NULL AS varchar), t.source_dataset
FROM {DB}.triple t JOIN {DB}.entity e ON e.uid = t.object_uid
WHERE t.subject_uid = '{u}' AND t.predicate_iri IN ('fin:holdsAccount', 'fin:isAuthorizedOn', 'fin:locatedIn'){_at('t', d)}
ORDER BY kind, valid_from""".strip()


def sql_entity(uid: str) -> str:
    return f"SELECT uid, class_iri, also_types, label, source_system, source_dataset, attributes, valid_from, valid_to " \
           f"FROM {DB}.entity WHERE uid = '{_uid(uid)}'"


def sql_neighborhood(uid: str, predicate_iri: Optional[str], direction: str, limit: int) -> str:
    u = _uid(uid)
    pred = f" AND t.predicate_iri = '{_cls(predicate_iri)}'" if predicate_iri else ""
    lim = max(1, min(int(limit), 100))
    out = (f"SELECT 'out' AS dir, t.triple_id, t.predicate_iri, t.object_uid AS other_uid, e.class_iri, e.label, "
           f"t.object_literal, t.weight, t.valid_from, t.valid_to, t.is_inferred, t.confidence "
           f"FROM {DB}.triple t LEFT JOIN {DB}.entity e ON e.uid = t.object_uid WHERE t.subject_uid = '{u}'{pred}")
    inc = (f"SELECT 'in' AS dir, t.triple_id, t.predicate_iri, t.subject_uid AS other_uid, e.class_iri, e.label, "
           f"t.object_literal, t.weight, t.valid_from, t.valid_to, t.is_inferred, t.confidence "
           f"FROM {DB}.triple t JOIN {DB}.entity e ON e.uid = t.subject_uid WHERE t.object_uid = '{u}'{pred}")
    parts = {"out": [out], "in": [inc], "both": [out, inc]}.get(direction)
    if parts is None:
        raise ToolError("direction phai la out | in | both")
    return " UNION ALL ".join(parts) + f" ORDER BY predicate_iri, valid_from LIMIT {lim}"


def sql_count_holders(product_class: str, region_label: Optional[str], as_of: Optional[str]) -> str:
    """CQ2: số khách giữ sản phẩm thuộc lớp X (gồm MỌI lớp con, qua bảng subclass)."""
    c, d = _cls(product_class), _date(as_of)
    region = ""
    if region_label:
        r = _text(region_label).lower()
        # khach -> locatedIn quan -> partOf vung (2 buoc, du cho cay Berka/corebank)
        region = f"""
  AND EXISTS (SELECT 1 FROM {DB}.triple l
              JOIN {DB}.triple po ON po.subject_uid = l.object_uid AND po.predicate_iri = 'fin:partOf'
              JOIN {DB}.entity rg ON rg.uid = po.object_uid
              WHERE l.subject_uid = h.subject_uid AND l.predicate_iri = 'fin:locatedIn'
                AND lower(rg.label) LIKE '%{r}%')"""
    return f"""
SELECT p.class_iri, p.label, c.source_system, count(DISTINCT h.subject_uid) AS customers
FROM {DB}.triple h
JOIN {DB}.triple o ON o.subject_uid = h.object_uid AND o.predicate_iri = 'fin:ofProduct'
JOIN {DB}.entity p ON p.uid = o.object_uid
JOIN {DB}.subclass s ON s.class_iri = p.class_iri AND s.ancestor_iri = '{c}'
JOIN {DB}.entity c ON c.uid = h.subject_uid
WHERE h.predicate_iri = 'fin:hasHolding'{_at('h', d)}{region}
GROUP BY p.class_iri, p.label, c.source_system ORDER BY customers DESC""".strip()


# ---------------------------------------------------------------- openCypher (Neptune)
# Mọi giá trị vào truy vấn đi qua `params` — không ghép chuỗi. Độ dài đường đi không tham số hoá được
# trong openCypher nên chèn SỐ NGUYÊN đã kiểm biên (int + min/max), không bao giờ chèn chuỗi.

LINK_TYPES = ("transfersTo", "isHeldBy", "involvedIn", "isDirectlyOwnedBy", "sameAs", "holdsAccount",
              "isAuthorizedOn", "atBank", "hasHolding", "ofProduct")
MAX_LINK_HOPS = 6


def cypher_explain_link(hops: int) -> str:
    h = max(1, min(int(hops), MAX_LINK_HOPS))
    rel = "|".join(LINK_TYPES)
    return (f"MATCH p = (a)-[:{rel}*{h}]-(b) WHERE id(a) = $a AND id(b) = $b "
            "RETURN [n IN nodes(p) | {uid: id(n), label: n.label, labels: labels(n)}] AS nodes, "
            "[r IN relationships(p) | {triple_id: id(r), type: type(r), from_uid: id(startNode(r)), "
            "to_uid: id(endNode(r)), weight: r.weight, confidence: r.confidence}] AS edges LIMIT 1")


CYPHER_AML_CASES = (
    "MATCH (a) WHERE id(a) = $uid "
    "OPTIONAL MATCH (a)-[i:involvedIn]->(v)-[h:hasTypology]->(t) "
    "OPTIONAL MATCH (a)-[o:isHeldBy]->(p) "
    "RETURN a.label AS label, labels(a) AS labels, "
    "collect(DISTINCT {attempt_uid: id(v), attempt: v.label, typology: t.label, detail: v.typology_detail, "
    "triple_id: id(i), typology_triple_id: id(h)}) AS attempts, "
    "collect(DISTINCT {owner_uid: id(p), owner: p.label, triple_id: id(o)}) AS owners")


def cypher_aml_transfers(hops: int) -> str:
    h = max(1, min(int(hops), 2))
    return (f"MATCH p = (a)-[:transfersTo*1..{h}]-(b) WHERE id(a) = $uid "
            "UNWIND relationships(p) AS r "
            "WITH DISTINCT r, startNode(r) AS s, endNode(r) AS e "
            "OPTIONAL MATCH (s)-[:involvedIn]->(vs) OPTIONAL MATCH (e)-[:involvedIn]->(ve) "
            "RETURN id(r) AS triple_id, id(s) AS from_uid, s.label AS from_label, id(e) AS to_uid, "
            "e.label AS to_label, r.weight AS n_txn, r.valid_from AS first_date, "
            "count(DISTINCT vs) > 0 AS from_flagged, count(DISTINCT ve) > 0 AS to_flagged "
            "ORDER BY n_txn DESC LIMIT $limit")


def check_readonly_sql(sql: str) -> str:
    s = sql.strip().rstrip(";")
    if ";" in s:
        raise ToolError("chi mot cau lenh")
    if not re.match(r"^(select|with)\b", s, re.IGNORECASE):
        raise ToolError("chi cho SELECT / WITH")
    if FORBIDDEN.search(s):
        raise ToolError("cau lenh co tu khoa ghi/DDL")
    dbs = {m.lower() for m in re.findall(r"\b([a-z_][a-z0-9_]*)\.[a-z_][a-z0-9_]*\b", s, re.IGNORECASE)}
    tables_dbs = {d for d in dbs if d.startswith(("track_", "silver_", "raw_"))}
    if tables_dbs - set(READ_DBS):
        raise ToolError(f"chi doc duoc {READ_DBS}, cau co {sorted(tables_dbs - set(READ_DBS))}")
    if not re.search(r"\blimit\s+\d+\s*$", s, re.IGNORECASE):
        s += f" LIMIT {MAX_ROWS}"
    return s


# ---------------------------------------------------------------- định nghĩa tool cho Claude

def _obj(props: Dict[str, Any], required: List[str]) -> Dict[str, Any]:
    # strict: true đòi additionalProperties false và MỌI trường trong required; trường tuỳ chọn = nullable
    return {"type": "object", "properties": props, "required": required, "additionalProperties": False}


_S = {"type": "string"}
_NS = {"type": ["string", "null"]}

TOOL_SPECS: List[Dict[str, Any]] = [
    {"name": "describe", "strict": True,
     "description": "Tra nghia mot khai niem cua ontology FinGraph theo ten tieng Viet/Anh, IRI (fin:Loan) hoac ma "
                    "nguon (santander:ind_tjcr_fin_ult1). Tra ve dinh nghia, lop cha/con, ma nguon, nguon co du lieu. "
                    "Goi truoc khi dung mot lop trong tool khac neu chua chac IRI.",
     "input_schema": _obj({"term": _S}, ["term"])},
    {"name": "find_entity", "strict": True,
     "description": "Tim node theo nhan (vd 'Khach 2', 'Tai khoan 10', 'Praha'). Tra uid de dung cho tool khac. "
                    "class_iri (tuy chon) loc theo lop, vd fin:Customer.",
     "input_schema": _obj({"text": _S, "class_iri": _NS, "limit": {"type": ["integer", "null"]}},
                          ["text", "class_iri", "limit"])},
    {"name": "customer_360", "strict": True,
     "description": "Ho so mot khach (uid): san pham dang giu, tai khoan so huu / duoc uy quyen, vi tri. as_of "
                    "(YYYY-MM-DD, tuy chon) = trang thai tai ngay do. Moi dong co triple_id de trich dan.",
     "input_schema": _obj({"uid": _S, "as_of": _NS}, ["uid", "as_of"])},
    {"name": "neighborhood", "strict": True,
     "description": "Cac canh mot buoc quanh mot node (uid). predicate_iri tuy chon (vd fin:transfersTo); "
                    "direction out | in | both. Dung de giai thich lien ket giua hai thuc the.",
     "input_schema": _obj({"uid": _S, "predicate_iri": _NS, "direction": {"type": "string", "enum": ["out", "in", "both"]},
                           "limit": {"type": ["integer", "null"]}}, ["uid", "predicate_iri", "direction", "limit"])},
    {"name": "count_holders", "strict": True,
     "description": "Dem so khach giu san pham thuoc mot lop (GOM MOI LOP CON, vd fin:CreditProduct gom the va vay), "
                    "tuy chon loc theo vung (nhan vung) va ngay as_of. Chia theo lop la va nguon du lieu.",
     "input_schema": _obj({"product_class": _S, "region": _NS, "as_of": _NS}, ["product_class", "region", "as_of"])},
    {"name": "query", "strict": True,
     "description": "SQL CHI DOC (Athena/Trino) tren track_a_onto (entity, triple, class, property, subclass, concept) "
                    "va track_a_gold. Chi dung khi cac tool tren khong du. Toi da 200 dong.",
     "input_schema": _obj({"sql": _S}, ["sql"])},
    {"name": "explain_link", "strict": True,
     "description": "Duong di NGAN NHAT giua hai node (uid_a, uid_b) tren do thi Neptune, qua chuyen tien, so huu, "
                    "tham gia vu rua tien, trung thuc the... toi da max_hops buoc (<= 6). Moi canh co triple_id. "
                    "Neptune chi chua VUNG DIEU TRA AML; khong thay duong thi noi ro la khong thay trong vung do.",
     "input_schema": _obj({"uid_a": _S, "uid_b": _S, "max_hops": {"type": ["integer", "null"]}},
                          ["uid_a", "uid_b", "max_hops"])},
    {"name": "aml_subgraph", "strict": True,
     "description": "Ho so dieu tra AML cua mot tai khoan (uid): vu rua tien + kieu hinh (FAN-OUT, CYCLE...), "
                    "chu so huu, va luoi chuyen tien quanh no trong hops buoc (1 hoac 2), danh dau doi tac da nam "
                    "trong vu nao.",
     "input_schema": _obj({"uid": _S, "hops": {"type": ["integer", "null"]}, "limit": {"type": ["integer", "null"]}},
                          ["uid", "hops", "limit"])},
]


class Tools:
    """Thực thi tool. Ghi lại mọi id đã trả về trong phiên -> dữ liệu cho kiểm grounding."""

    def __init__(self, backend: Backend, cat: Optional[Dict[str, Any]] = None,
                 log: Optional[Callable[[Dict[str, Any]], None]] = None, graph: Optional[GraphBackend] = None):
        self.backend = backend
        self.graph = graph
        self.cat = cat if cat is not None else C.load()
        self.seen_ids: set = set()
        self.log = log or (lambda rec: None)

    def _rows(self, sql: str) -> List[Dict[str, Any]]:
        rows = self.backend.run_sql(sql)
        for r in rows:
            for k in ("uid", "triple_id", "item_uid", "other_uid", "subject_uid", "object_uid"):
                v = r.get(k)
                if isinstance(v, str) and UID.match(v):
                    self.seen_ids.add(v)
        return rows

    def _collect(self, obj: Any) -> None:
        """Thu MỌI id 32 hex trong kết quả lồng nhau (đường đi, danh sách cạnh) cho kiểm grounding."""
        if isinstance(obj, dict):
            for v in obj.values():
                self._collect(v)
        elif isinstance(obj, list):
            for v in obj:
                self._collect(v)
        elif isinstance(obj, str) and UID.match(obj):
            self.seen_ids.add(obj)

    def _cypher(self, query: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        if self.graph is None:
            raise ToolError("Neptune chua bat (scripts/neptune-session.ps1 start); dung neighborhood (Athena) thay the")
        rows = self.graph.run_cypher(query, params)
        self._collect(rows)
        return rows

    def call(self, name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        t0 = time.monotonic()
        try:
            out = self._dispatch(name, args)
            ok = True
        except ToolError as e:
            out, ok = {"error": str(e)}, False
        self.log({"tool": name, "args": args, "ok": ok, "rows": len(out.get("rows", [])) if ok else 0,
                  "ms": int((time.monotonic() - t0) * 1000)})
        return out if ok else {**out, "_is_error": True}

    def _dispatch(self, name: str, a: Dict[str, Any]) -> Dict[str, Any]:
        if name == "describe":
            return {"rows": C.describe(self.cat, _text(a["term"], 120).replace("''", "'"))}
        if name == "find_entity":
            return {"rows": self._rows(sql_find_entity(a["text"], a.get("class_iri"), a.get("limit") or 10))}
        if name == "customer_360":
            ent = self._rows(sql_entity(a["uid"]))
            if not ent:
                raise ToolError("khong co node voi uid nay")
            return {"entity": ent[0], "rows": self._rows(sql_customer_360(a["uid"], a.get("as_of")))}
        if name == "neighborhood":
            return {"rows": self._rows(sql_neighborhood(a["uid"], a.get("predicate_iri"), a["direction"],
                                                        a.get("limit") or 50))}
        if name == "count_holders":
            return {"rows": self._rows(sql_count_holders(a["product_class"], a.get("region"), a.get("as_of")))}
        if name == "query":
            return {"rows": self._rows(check_readonly_sql(a["sql"]))}
        if name == "explain_link":
            ua, ub = _uid(a["uid_a"]), _uid(a["uid_b"])
            top = max(1, min(int(a.get("max_hops") or 4), MAX_LINK_HOPS))
            for h in range(1, top + 1):          # tang dan do dai = duong ngan nhat (Neptune khong co shortestPath)
                rows = self._cypher(cypher_explain_link(h), {"a": ua, "b": ub})
                if rows:
                    return {"hops": h, "rows": rows}
            return {"hops": None, "rows": [], "note": f"khong co duong <= {top} buoc trong vung do thi tren Neptune"}
        if name == "aml_subgraph":
            u = _uid(a["uid"])
            cases = self._cypher(CYPHER_AML_CASES, {"uid": u})
            if not cases:
                raise ToolError("khong co node nay tren Neptune (ngoai vung dieu tra?)")
            lim = max(1, min(int(a.get("limit") or 50), 200))
            transfers = self._cypher(cypher_aml_transfers(a.get("hops") or 1), {"uid": u, "limit": lim})
            return {"node": cases[0], "rows": transfers}
        raise ToolError(f"khong co tool {name}")


class AthenaBackend:
    """Chạy SQL qua Athena dưới workgroup của persona (trần quét, nơi ghi kết quả).

    Credential là của PERSONA người hỏi (assume role trước khi tạo session boto3): Lake Formation
    lọc dữ liệu ở tầng lưu trữ, nên prompt injection không đọc được thứ persona không được đọc.
    """

    def __init__(self, workgroup: str, session=None, region: str = "ap-southeast-1", timeout_s: int = 60):
        import boto3

        self.athena = (session or boto3.Session()).client("athena", region_name=region)
        self.workgroup, self.timeout_s = workgroup, timeout_s

    def run_sql(self, sql: str) -> List[Dict[str, Any]]:
        qid = self.athena.start_query_execution(QueryString=sql, WorkGroup=self.workgroup)["QueryExecutionId"]
        deadline = time.monotonic() + self.timeout_s
        while True:
            st = self.athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
            if st["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            if time.monotonic() > deadline:
                self.athena.stop_query_execution(QueryExecutionId=qid)
                raise ToolError(f"truy van qua {self.timeout_s}s, hay thu hep dieu kien")
            time.sleep(0.5)
        if st["State"] != "SUCCEEDED":
            raise ToolError(f"Athena {st['State']}: {st.get('StateChangeReason', '')[:300]}")
        res = self.athena.get_query_results(QueryExecutionId=qid, MaxResults=MAX_ROWS + 1)
        rows = res["ResultSet"]["Rows"]
        head = [c.get("VarCharValue") for c in rows[0]["Data"]] if rows else []
        return [dict(zip(head, [c.get("VarCharValue") for c in r["Data"]])) for r in rows[1:]]


class NeptuneBackend:
    """openCypher qua Lambda `<prefix>-neptune-ops` (action `query`, chặn từ khoá ghi): agent chạy ngoài VPC vẫn
    dùng được Neptune mà không mở cổng 8182. Quyền cần: lambda:InvokeFunction trên đúng hàm này."""

    def __init__(self, function_name: str = "fingraph-dev-neptune-ops", session=None, region: str = "ap-southeast-1"):
        import boto3

        self.fn = function_name
        self.lam = (session or boto3.Session()).client("lambda", region_name=region)

    def run_cypher(self, query: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        import json

        r = self.lam.invoke(FunctionName=self.fn, Payload=json.dumps(
            {"action": "query", "cypher": query, "params": params}).encode())
        body = json.loads(r["Payload"].read() or b"{}")
        if "results" not in body:
            raise ToolError(f"Neptune loi: {json.dumps(body, ensure_ascii=False)[:300]}")
        return body["results"]
