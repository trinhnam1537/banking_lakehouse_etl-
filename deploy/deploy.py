"""Deploy code ETL lên một môi trường — KHÔNG dùng Terraform.

Repo hạ tầng dựng sẵn nền tảng và công bố tên mọi thứ ở SSM
`/fingraph/<env>/platform/*`. Script này đọc các tên đó rồi:

  1. build etl.zip, upload  _ops/etl/releases/<sha>/etl.zip
  2. upload bootstrap       _ops/etl/bootstrap/glue_bootstrap.py   (khoá cố định)
     upload DDL silver      _ops/silver/ddl/**.sql
     upload script Glue     _ops/glue-scripts/*.py   (bronze_ingest, onto_closure — khoá
                            cố định mà module glue_jobs của repo hạ tầng trỏ tới)
  3. update-function-code   mọi Lambda trong platform/lambda_functions
  4. con trỏ release        SSM <release_param> = <sha>
  5. config-sync            YAML -> bảng job_config (src, slv)

Thứ tự có chủ ý: code mới lên TRƯỚC (1-4), cấu hình SAU (5). Cấu hình mới có
thể dùng khoá mà chỉ code mới hiểu; ngược lại thì code mới vẫn đọc được cấu
hình cũ. Lần chạy Glue nào cũng ghim một release cụ thể, nên đổi con trỏ giữa
lúc job đang chạy không ảnh hưởng job đó.

    python -m deploy.deploy --env dev --sha <commit> [--dry-run]
"""

from __future__ import annotations

import argparse
import os
import pathlib
import re
import sys
import tempfile
from typing import Any, Dict, List, Optional

from . import config_sync
from .build import REPO, build

RELEASE_RE = re.compile(r"^[0-9a-f]{7,40}$")
RELEASE_PREFIX = "_ops/etl/releases"
BOOTSTRAP_KEY = "_ops/etl/bootstrap/glue_bootstrap.py"
DDL_PREFIX = "_ops/silver/ddl"
GLUE_SCRIPTS_PREFIX = "_ops/glue-scripts"

REQUIRED = ("artifact_bucket", "src_config_table", "slv_config_table",
            "lambda_functions", "release_param")


def platform_params(env: str, ssm=None) -> Dict[str, str]:
    """Đọc /fingraph/<env>/platform/* — hợp đồng duy nhất giữa hai repo."""
    import boto3

    ssm = ssm or boto3.client("ssm")
    path = f"/fingraph/{env}/platform/"
    out: Dict[str, str] = {}
    kwargs: Dict[str, Any] = {"Path": path, "Recursive": True}
    while True:
        page = ssm.get_parameters_by_path(**kwargs)
        for p in page.get("Parameters", []):
            out[p["Name"][len(path):]] = p["Value"]
        if "NextToken" not in page:
            break
        kwargs["NextToken"] = page["NextToken"]
    missing = [k for k in REQUIRED if not out.get(k)]
    if missing:
        raise SystemExit(f"Thieu tham so nen tang trong SSM {path}: {', '.join(missing)}. "
                         f"Repo ha tang da apply chua?")
    return out


def _functions(params: Dict[str, str]) -> List[str]:
    return [f.strip() for f in params["lambda_functions"].split(",") if f.strip()]


def deploy(env: str, sha: str, dry_run: bool = False, root: pathlib.Path = REPO,
           session=None, log=print) -> Dict[str, Any]:
    if not RELEASE_RE.match(sha):
        raise SystemExit(f"sha khong hop le: {sha!r}")
    import boto3

    session = session or boto3.session.Session()
    s3, lam, ssm = session.client("s3"), session.client("lambda"), session.client("ssm")
    ddb = session.resource("dynamodb")

    params = platform_params(env, ssm)
    bucket = params["artifact_bucket"]
    key = f"{RELEASE_PREFIX}/{sha}/etl.zip"
    tag = "[dry-run] " if dry_run else ""

    # Kiem tra cau hinh TRUOC moi thu: entry sai thi dung, chua upload gi
    src_items = config_sync.load_sourcing(str(root / "sourcing_etl" / "sources"))
    slv_items = config_sync.load_silver(str(root / "silver_etl" / "entries"))
    log(f"Cau hinh hop le: {len(src_items)} entry sourcing, {len(slv_items)} entry silver")

    with tempfile.TemporaryDirectory() as tmp:
        zip_path = build(pathlib.Path(tmp) / "etl.zip", root)
        log(f"{tag}1. s3://{bucket}/{key} ({zip_path.stat().st_size} bytes)")
        if not dry_run:
            s3.upload_file(str(zip_path), bucket, key)

    boot = root / "deploy" / "glue_bootstrap.py"
    log(f"{tag}2. s3://{bucket}/{BOOTSTRAP_KEY}")
    ddl_files = sorted((root / "silver_etl" / "ddl").rglob("*.sql"))
    if not dry_run:
        s3.upload_file(str(boot), bucket, BOOTSTRAP_KEY)
        for f in ddl_files:
            rel = f.relative_to(root / "silver_etl" / "ddl").as_posix()
            s3.upload_file(str(f), bucket, f"{DDL_PREFIX}/{rel}",
                           ExtraArgs={"ContentType": "text/plain"})
    log(f"{tag}   + {len(ddl_files)} file DDL -> s3://{bucket}/{DDL_PREFIX}/")

    glue_scripts = sorted((root / "jobs" / "glue").glob("*.py"))
    if not dry_run:
        for f in glue_scripts:
            s3.upload_file(str(f), bucket, f"{GLUE_SCRIPTS_PREFIX}/{f.name}")
    log(f"{tag}   + {len(glue_scripts)} script Glue -> s3://{bucket}/{GLUE_SCRIPTS_PREFIX}/")

    for fn in _functions(params):
        log(f"{tag}3. Lambda {fn} <- {key}")
        if not dry_run:
            lam.update_function_code(FunctionName=fn, S3Bucket=bucket, S3Key=key)
            lam.get_waiter("function_updated_v2").wait(FunctionName=fn)

    log(f"{tag}4. SSM {params['release_param']} = {sha}")
    if not dry_run:
        ssm.put_parameter(Name=params["release_param"], Value=sha, Type="String", Overwrite=True)

    log(f"{tag}5. config-sync")
    reports = [
        config_sync.sync(params["src_config_table"], src_items, dry_run, ddb, log),
        config_sync.sync(params["slv_config_table"], slv_items, dry_run, ddb, log),
    ]
    return {"env": env, "sha": sha, "bucket": bucket, "key": key, "dry_run": dry_run,
            "functions": _functions(params), "reports": reports}


def _summary(result: Dict[str, Any]) -> str:
    lines = [f"## Deploy ETL `{result['sha'][:7]}` -> {result['env']}"
             + (" (dry-run)" if result["dry_run"] else ""),
             "", f"- Code: `s3://{result['bucket']}/{result['key']}`",
             f"- Lambda: {', '.join(result['functions']) or '(khong co)'}"]
    lines += [f"- {r.summary()}" for r in result["reports"]]
    return "\n".join(lines) + "\n"


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Deploy code ETL (khong dung Terraform)")
    ap.add_argument("--env", required=True, choices=["dev", "prd"])
    ap.add_argument("--sha", required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)

    result = deploy(args.env, args.sha, args.dry_run)
    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a", encoding="utf-8") as f:
            f.write(_summary(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
