"""Nghiệm thu dữ liệu đồ thị trên AWS (ONTOLOGY-BUILD.md O5-O11): đối soát bằng SQL, CHỈ ĐỌC.

    python scripts/onto_acceptance.py --profile fg-de-mcp [--neptune fingraph-dev-neptune-ops --admin-profile fingraph-mcp]

Chạy dưới persona data engineer (đọc track_a_onto + silver + gold). Mỗi kiểm tra in số liệu và ĐẠT/TRƯỢT; ghi báo cáo
docs/onto-acceptance.md (bảng số node/cạnh theo lớp và predicate + kết quả từng mục checklist). Không in dữ liệu khách.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DB = "track_a_onto"
# Cặp nguồn CÓ khoá chung (ONTOLOGY-BUILD.md O2): sameAs chỉ được phép giữa các cặp này
ALLOWED_SAMEAS = {frozenset({"berka", "corebank"}), frozenset({"corebank"}), frozenset({"berka"}),
                  frozenset({"opensanctions"}), frozenset({"corebank", "opensanctions"}),
                  frozenset({"berka", "opensanctions"})}
GOLD_TO_CLASS = {"loan": "fin:Loan", "card_classic": "fin:ClassicCard", "card_junior": "fin:JuniorCard",
                 "card_gold": "fin:GoldCard", "household": "fin:RecurringPayment", "insurance": "fin:InsuranceProduct"}


def main(argv=None) -> int:
    import boto3

    from agent.tools import AthenaBackend, NeptuneBackend, sql_customer_360

    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="fg-de-mcp")
    ap.add_argument("--workgroup", default="fingraph-dev-data-engineer")
    ap.add_argument("--neptune", default=None, help="Lambda neptune-ops: doi soat dinh/canh Neptune")
    ap.add_argument("--admin-profile", default="fingraph-mcp", help="profile goi Lambda neptune-ops")
    ap.add_argument("--out", default=str(ROOT / "docs" / "onto-acceptance.md"))
    a = ap.parse_args(argv)
    sys.stdout.reconfigure(encoding="utf-8")
    be = AthenaBackend(a.workgroup, session=boto3.Session(profile_name=a.profile), timeout_s=300)

    def q(sql):
        t = time.time()
        rows = be.run_sql(sql)
        print(f"  ({len(rows)} dong, {time.time() - t:.0f}s)")
        return rows

    checks, md = [], []

    def check(item, ok, evidence):
        checks.append({"item": item, "ok": ok, "evidence": evidence})
        print(f"[{'DAT' if ok else 'TRUOT'}] {item}: {evidence}")

    run = q(f"SELECT run_id, count(*) n FROM {DB}.entity GROUP BY run_id ORDER BY n DESC")
    trun = q(f"SELECT run_id, count(*) n FROM {DB}.triple GROUP BY run_id ORDER BY n DESC")
    check("O7: entity/triple cùng một run_id", len(run) == 1 and len(trun) == 1 and run[0]["run_id"] == trun[0]["run_id"],
          f"entity run_id {[r['run_id'] for r in run]}, triple run_id {[r['run_id'] for r in trun]}")

    nodes = q(f"SELECT class_iri, source_system, count(*) n FROM {DB}.entity GROUP BY 1, 2 ORDER BY 3 DESC")
    edges = q(f"SELECT predicate_iri, is_inferred, count(*) n FROM {DB}.triple GROUP BY 1, 2 ORDER BY 3 DESC")
    n_nodes, n_edges = sum(int(r["n"]) for r in nodes), sum(int(r["n"]) for r in edges)
    check("O7: số node/cạnh theo lớp và predicate đã ghi", n_nodes > 0 and n_edges > 0,
          f"{n_nodes:,} node, {n_edges:,} cạnh (bảng chi tiết bên dưới)")

    from transforms.onto.mapping import TIMELESS

    nvf = q(f"SELECT predicate_iri, count(*) n FROM {DB}.triple WHERE valid_from IS NULL GROUP BY 1 ORDER BY 2 DESC")
    undeclared = [r["predicate_iri"] for r in nvf if r["predicate_iri"] not in TIMELESS]
    check("O6: mọi cạnh có valid_from, hoặc thuộc quy tắc cạnh không thời điểm (mapping.TIMELESS)", not undeclared,
          f"cạnh thiếu valid_from: {sum(int(r['n']) for r in nvf):,} ở {[r['predicate_iri'] for r in nvf]}; "
          f"chưa khai quy tắc: {undeclared or 'không'}")

    pairs = q(f"""SELECT ea.source_system sa, eb.source_system sb, count(*) n, min(s.confidence) cmin
                  FROM {DB}.same_as s JOIN {DB}.entity ea ON ea.uid = s.uid_a JOIN {DB}.entity eb ON eb.uid = s.uid_b
                  GROUP BY 1, 2""")
    bad = [p for p in pairs if frozenset({p["sa"], p["sb"]}) not in ALLOWED_SAMEAS]
    check("O8: không có same_as giữa hai vũ trụ không có khoá chung", not bad,
          f"cặp nguồn: {[(p['sa'], p['sb'], int(p['n'])) for p in pairs]}; ngoài danh sách cho phép: {bad}")
    check("O8: same_as đã xác nhận đều có confidence >= 0.9",
          all(float(p["cmin"]) >= 0.9 for p in pairs), f"confidence nhỏ nhất theo cặp: {[p['cmin'] for p in pairs]}")
    rv = q(f"SELECT count(*) n, min(confidence) lo, max(confidence) hi FROM {DB}.same_as_review")[0]
    check("O8: hàng đợi duyệt same_as_review có confidence trong [0.6, 0.9)",
          int(rv["n"]) > 0 and float(rv["lo"]) >= 0.6 and float(rv["hi"]) < 0.9,
          f"{rv['n']} ứng viên, confidence {rv['lo']}..{rv['hi']}")

    cl = q(f"""SELECT max(depth) dmax, sum(CASE WHEN start_uid = node_uid AND depth > 0 THEN 1 ELSE 0 END) self_loops,
                      count(*) n FROM {DB}.closure""")[0]
    check("O9: closure không chu trình, độ sâu <= MAX_DEPTH (6)",
          int(cl["self_loops"] or 0) == 0 and int(cl["dmax"] or 0) <= 6,
          f"{cl['n']} dòng, độ sâu lớn nhất {cl['dmax']}, vòng về chính nó {cl['self_loops']}")

    inf = q(f"""SELECT predicate_iri, count(*) n, sum(CASE WHEN mapping_id IS NULL OR source_key IS NULL THEN 1 ELSE 0 END) untraced,
                       min(mapping_id) rule FROM {DB}.triple WHERE is_inferred GROUP BY 1""")
    check("O9: mỗi cạnh suy diễn truy ngược được về luật (mapping_id) và cạnh gốc (source_key)",
          all(int(r["untraced"] or 0) == 0 for r in inf),
          f"{[(r['predicate_iri'], int(r['n']), r['rule'], int(r['untraced'] or 0)) for r in inf] or 'không có cạnh suy diễn'}")

    gold = {r["product"]: int(r["n"]) for r in q(
        "SELECT product, count(DISTINCT client_id) n FROM track_a_gold.berka_product_event GROUP BY 1")}
    graph = {r["class_iri"]: int(r["n"]) for r in q(f"""
        SELECT p.class_iri, count(DISTINCT h.subject_uid) n FROM {DB}.triple h
        JOIN {DB}.triple o ON o.subject_uid = h.object_uid AND o.predicate_iri = 'fin:ofProduct'
        JOIN {DB}.entity p ON p.uid = o.object_uid JOIN {DB}.entity c ON c.uid = h.subject_uid
        WHERE h.predicate_iri = 'fin:hasHolding' AND c.source_system = 'berka' GROUP BY 1""")}
    cmp_rows = [(g, gold[g], graph.get(c, 0)) for g, c in GOLD_TO_CLASS.items() if g in gold]
    check("O5: sản phẩm Berka trên đồ thị khớp track_a_gold.berka_product_event (số khách theo sản phẩm)",
          all(x == y for _, x, y in cmp_rows), f"(sản phẩm, gold, đồ thị): {cmp_rows}")

    who = q(f"SELECT uid FROM {DB}.entity WHERE class_iri = 'fin:Customer' AND source_system = 'berka' "
            f"AND label = 'Khách 2' LIMIT 1")
    if who:
        c360 = q(sql_customer_360(who[0]["uid"], None))
        kinds = sorted({r["kind"] for r in c360})
        check("O7: dựng được hồ sơ 360 một khách chỉ bằng SQL trên entity + triple", bool(c360),
              f"Khách 2 (Berka): {len(c360)} dòng, loại {kinds}")
    else:
        check("O7: dựng được hồ sơ 360 một khách chỉ bằng SQL trên entity + triple", False, "không tìm thấy Khách 2")

    rec = q(f"SELECT count(*) n FROM {DB}.entity WHERE class_iri = 'fin:Recommendation'")[0]
    check("O10: Recommendation có trong đồ thị", int(rec["n"]) > 0, f"{rec['n']} node fin:Recommendation")

    if a.neptune:
        nb = NeptuneBackend(a.neptune, session=boto3.Session(profile_name=a.admin_profile))
        nv = nb.run_cypher("MATCH (n) RETURN count(n) AS n", {})[0]["n"]
        ne = nb.run_cypher("MATCH ()-[r]->() RETURN count(r) AS n", {})[0]["n"]
        # Neptune là vùng điều tra (AML thu hẹp) + mọi nguồn khác: so với Iceberg theo nguồn không phải AML
        by_src = {}
        for r in nodes:
            by_src[r["source_system"]] = by_src.get(r["source_system"], 0) + int(r["n"])
        non_aml_nodes = sum(v for k, v in by_src.items() if k != "aml")
        nep_non_aml = nb.run_cypher("MATCH (n) WHERE n.source_system <> 'aml' RETURN count(n) AS n", {})[0]["n"]
        gap = non_aml_nodes - int(nep_non_aml)
        why = [k for k, v in by_src.items() if k != "aml" and v == gap]
        check("NEPTUNE: đỉnh ngoài vùng AML khớp entity Iceberg (cùng run)", gap == 0,
              f"Neptune {nv:,} đỉnh / {ne:,} cạnh; đỉnh không phải AML: Neptune {nep_non_aml:,} vs Iceberg "
              f"{non_aml_nodes:,}; lệch {gap:,}" + (f" = đúng số đỉnh nguồn {why}: Neptune nạp từ bản export TRƯỚC khi "
                                                   f"đồ thị có nguồn này, cần export + nạp lại" if why else ""))

    today = dt.date.today().isoformat()
    md += [f"# Nghiệm thu dữ liệu đồ thị FinGraph ({today})", "",
           "Sinh bởi `scripts/onto_acceptance.py` (persona data engineer, chỉ đọc). "
           "Kiểm tra mã nguồn và logic nằm ở `pytest tests/ontology tests/transforms` (96 test đạt ngày 11-19).", "",
           "## Kết quả kiểm tra", "", "| Mục | Kết quả | Bằng chứng |", "|---|---|---|"]
    md += [f"| {c['item']} | {'đạt' if c['ok'] else '**trượt**'} | {c['evidence']} |" for c in checks]
    md += ["", f"## Số node theo lớp và nguồn ({n_nodes:,})", "", "| Lớp | Nguồn | Số node |", "|---|---|---:|"]
    md += [f"| `{r['class_iri']}` | {r['source_system']} | {int(r['n']):,} |" for r in nodes]
    md += ["", f"## Số cạnh theo predicate ({n_edges:,})", "", "| Predicate | Suy diễn | Số cạnh |", "|---|---|---:|"]
    md += [f"| `{r['predicate_iri']}` | {r['is_inferred']} | {int(r['n']):,} |" for r in edges]
    pathlib.Path(a.out).write_text("\n".join(md) + "\n", encoding="utf-8")
    (pathlib.Path(a.out).with_suffix(".json")).write_text(json.dumps(checks, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"Bao cao: {a.out}")
    return 0 if all(c["ok"] for c in checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
