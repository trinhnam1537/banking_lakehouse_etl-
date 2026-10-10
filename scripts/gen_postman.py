"""Sinh Postman collection tu cac file YAML trong sourcing_etl/sources.

Moi entry `source_type: api` thanh mot request, gom thu muc theo nha cung cap
(alphavantage, binance...). Entry khong phai HTTP (kafka, s3, postgres) bi bo qua.

URL ghep giong het sourcing/readers/api.py: base_url + path + query + tham so
trang dau tien. Khoa bi mat KHONG doc tu Secrets Manager — moi `secret_inject`
thanh mot bien collection rong (vd {{alphavantage_apikey}}), tu dien trong Postman.

Chay:
    python scripts/gen_postman.py
    python scripts/gen_postman.py --out postman/fingraph.postman_collection.json
"""
import argparse
import json
import re
import sys
import uuid
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
SOURCES = ROOT / "sourcing_etl" / "sources"
SINCE = "{since}"  # placeholder CDC1, xem sourcing/readers/api.py

# Kiem tra nhanh sau moi request: HTTP 200, dem so ban ghi theo records_path,
# canh bao khi Alpha Vantage het han muc (van tra 200 nhung body chi co Note).
TEST_SCRIPT = """
pm.test("HTTP 200", () => pm.response.to.have.status(200));
const body = pm.response.json();
if (body && (body.Note || body.Information || body["Error Message"])) {
    console.warn(body.Note || body.Information || body["Error Message"]);
}
const path = pm.variables.get("records_path") || "";
let node = body;
for (const p of path.split(".").filter(Boolean)) node = node && node[p];
pm.test("co ban ghi tai '" + path + "'", () => {
    pm.expect(node, "records_path khong ton tai").to.not.be.undefined;
    const n = Array.isArray(node) ? node.length : 1;
    console.log(pm.info.requestName + ": " + n + " ban ghi");
    pm.expect(n).to.be.above(0);
});
""".strip().splitlines()


def secret_var(secret_name: str, key: str) -> str:
    """fingraph/alphavantage + apikey -> alphavantage_apikey."""
    slug = secret_name.split("/", 1)[-1]
    return re.sub(r"[^A-Za-z0-9]+", "_", f"{slug}_{key}").strip("_")


def build_request(cfg: dict, variables: dict) -> dict:
    api = cfg["api"]
    base = api["base_url"].rstrip("/")
    path = (api.get("path") or "").lstrip("/")
    raw_url = f"{base}/{path}" if path else base

    query = []
    for k, v in (api.get("query") or {}).items():
        if v == SINCE:
            # Engine bo tham so nay khi chua co moc; de san nhung tat di.
            query.append({"key": k, "value": "", "disabled": True,
                          "description": "CDC1: moc thoi gian, engine tu dien"})
        else:
            query.append({"key": k, "value": str(v)})

    # Chi lay trang dau — Postman dung de xem hinh dang du lieu, khong phai keo het.
    mode = api.get("pagination", "none")
    size = api.get("page_size", 100)
    if mode in ("page", "offset"):
        first = 1 if mode == "page" else 0
        query.append({"key": api.get("page_param", "page"), "value": str(first)})
        query.append({"key": api.get("page_size_param", "limit"), "value": str(size)})

    headers = [{"key": k, "value": str(v)} for k, v in (api.get("headers") or {}).items()]

    for secret_key, dest in (api.get("secret_inject") or {}).items():
        var = secret_var(api["secret_name"], secret_key)
        variables.setdefault(var, "")
        where, _, target = dest.partition(".")
        entry = {"key": target, "value": "{{%s}}" % var}
        (headers if where == "header" else query).append(entry)

    active = [q for q in query if not q.get("disabled")]
    if active:
        raw_url += "?" + "&".join(f"{q['key']}={q['value']}" for q in active)

    status = "" if cfg.get("enabled", True) else " [disabled]"
    desc = [cfg.get("description", ""), "",
            f"- entry: `{cfg['name']}`{status}",
            f"- records_path: `{api.get('records_path', '')}`",
            f"- pagination: `{mode}` (chi trang dau)",
            f"- rate_limit_per_sec: {api.get('rate_limit_per_sec', '?')}"]

    return {
        "name": cfg["name"] + status,
        "event": [
            {"listen": "prerequest", "script": {"type": "text/javascript", "exec": [
                f"pm.variables.set('records_path', {json.dumps(api.get('records_path', ''))});"
            ]}},
            {"listen": "test", "script": {"type": "text/javascript", "exec": TEST_SCRIPT}},
        ],
        "request": {
            "method": api.get("method", "GET"),
            "header": headers,
            "url": {
                "raw": raw_url,
                "protocol": base.split("://")[0],
                "host": base.split("://")[1].split("/")[0].split("."),
                "path": [p for p in (base.split("://")[1].split("/")[1:] + path.split("/")) if p],
                "query": query,
            },
            "description": "\n".join(desc),
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--sources", type=Path, default=SOURCES)
    ap.add_argument("--out", type=Path, default=ROOT / "postman" / "fingraph.postman_collection.json")
    args = ap.parse_args()

    folders: dict = {}
    variables: dict = {}
    skipped = []

    for f in sorted(args.sources.rglob("*.yml")):
        cfg = yaml.safe_load(f.read_text(encoding="utf-8")) or {}
        if cfg.get("source_type") != "api" or "api" not in cfg:
            skipped.append(f"{f.name} ({cfg.get('source_type', '?')})")
            continue
        rel = f.parent.relative_to(args.sources)
        folder = rel.parts[0] if rel.parts else "misc"
        folders.setdefault(folder, []).append(build_request(cfg, variables))

    collection = {
        "info": {
            "_postman_id": str(uuid.uuid4()),
            "name": "FinGraph sources",
            "description": "Sinh tu dong boi scripts/gen_postman.py — dung sua tay, chay lai script.",
            "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json",
        },
        "item": [{"name": k, "item": v} for k, v in sorted(folders.items())],
        "variable": [{"key": k, "value": v} for k, v in sorted(variables.items())],
    }

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(collection, indent=2, ensure_ascii=False), encoding="utf-8")

    total = sum(len(v) for v in folders.values())
    print(f"Da ghi {total} request / {len(folders)} thu muc -> {args.out.relative_to(ROOT)}")
    if variables:
        print("Can dien bien trong Postman:", ", ".join(variables))
    if skipped:
        print("Bo qua (khong phai HTTP):", ", ".join(skipped))
    return 0


if __name__ == "__main__":
    sys.exit(main())
