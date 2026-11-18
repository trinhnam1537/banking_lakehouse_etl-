"""Benchmark pipeline từ số liệu ĐÃ GHI của mỗi lần chạy (không chạy thêm job nào) -> s3://<bucket>/_ops/benchmark/.

    .\\.venv\\Scripts\\python.exe scripts\\benchmark_report.py --env dev --s3 s3://fingraph-lakehouse-dev-439024108643/_ops/benchmark/

Nguồn:
  - bảng <prefix>-src-job-log, <prefix>-slv-job-log (DynamoDB): mỗi lần chạy sourcing / silver ghi thời gian,
    số bản ghi đọc/ghi, byte, số file (sourcing/runner.py, silver runner). Chỉ lấy dòng RUN#, status SUCCESS.
  - Glue get_job_runs của các job Spark tầng gold / pretrain / onto: thời gian chạy, DPU-giờ (Glue tự tính).

Ra: bảng theo job (số lần, trung vị / p95 thời gian, tổng bản ghi, bản ghi/giây) + bảng Glue (DPU-giờ, ước
chi phí 0,44 USD/DPU-giờ ở ap-southeast-1, kiểm lại trên trang giá Glue trước khi đưa vào luận văn).
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import pathlib
import statistics
import sys
from collections import defaultdict
from typing import Any, Dict, Iterable, List

GLUE_USD_PER_DPU_HOUR = 0.44
SPARK_JOB_SUFFIXES = ("gold-spark", "pretrain-spark", "onto-spark", "slv-spark", "src-spark")


def _f(v: Any) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def pct(xs: List[float], q: float) -> float:
    if not xs:
        return 0.0
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(q * (len(xs) - 1))))]


def scan(table) -> Iterable[Dict[str, Any]]:
    kw: Dict[str, Any] = {}
    while True:
        r = table.scan(**kw)
        yield from r["Items"]
        if "LastEvaluatedKey" not in r:
            return
        kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]


def run_rows(items: Iterable[Dict[str, Any]], layer: str) -> List[Dict[str, Any]]:
    """Dòng RUN# thành công -> {layer, job, started_at, seconds, records, bytes, files}."""
    out = []
    for i in items:
        if not str(i.get("run_key", "")).startswith("RUN#") or i.get("status") != "SUCCESS":
            continue
        m = i.get("metrics") or {}
        if layer == "src":
            rec, byt, files = _f(i.get("records_written")), _f(i.get("bytes_written")), _f(i.get("files_written"))
        else:
            rec, byt, files = _f(m.get("rows_out")), 0.0, _f(m.get("files"))
        out.append({"layer": layer, "job": i["job_name"], "started_at": i.get("started_at", ""),
                    "seconds": _f(i.get("duration_seconds")), "records": rec, "bytes": byt, "files": files})
    return out


def summarize(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by[(r["layer"], r["job"])].append(r)
    out = []
    for (layer, job), rs in sorted(by.items()):
        secs = [r["seconds"] for r in rs]
        rec = sum(r["records"] for r in rs)
        out.append({"layer": layer, "job": job, "runs": len(rs), "p50_s": statistics.median(secs),
                    "p95_s": pct(secs, 0.95), "total_s": sum(secs), "records": rec,
                    "records_per_s": rec / sum(secs) if sum(secs) else 0.0,
                    "mb": sum(r["bytes"] for r in rs) / 1e6})
    return out


def glue_runs(glue, job_names: List[str]) -> List[Dict[str, Any]]:
    out = []
    for name in job_names:
        kw: Dict[str, Any] = {"JobName": name}
        while True:
            r = glue.get_job_runs(**kw)
            for jr in r["JobRuns"]:
                if jr.get("JobRunState") != "SUCCEEDED":
                    continue
                args = jr.get("Arguments", {})
                out.append({"glue_job": name, "transform": args.get("--TRANSFORM") or args.get("--JOB_NAME", ""),
                            "started_at": str(jr.get("StartedOn", "")), "seconds": jr.get("ExecutionTime", 0),
                            "workers": jr.get("NumberOfWorkers", 0), "worker_type": jr.get("WorkerType", ""),
                            "dpu_hours": _f(jr.get("DPUSeconds")) / 3600 if jr.get("DPUSeconds")
                            else _f(jr.get("MaxCapacity")) * jr.get("ExecutionTime", 0) / 3600})
            if not r.get("NextToken"):
                break
            kw["NextToken"] = r["NextToken"]
    return out


def md_table(rows: List[Dict[str, Any]], cols: List[str]) -> str:
    def fmt(v):
        return f"{v:,.2f}" if isinstance(v, float) else str(v)
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    lines += ["| " + " | ".join(fmt(r[c]) for c in cols) + " |" for r in rows]
    return "\n".join(lines) + "\n"


def to_csv(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return ""
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=list(rows[0]), lineterminator="\n")
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Benchmark pipeline tu job log + Glue")
    ap.add_argument("--env", default="dev")
    ap.add_argument("--project", default="fingraph")
    ap.add_argument("--region", default="ap-southeast-1")
    ap.add_argument("--out", default="benchmark-report")
    ap.add_argument("--s3", default=None, help="s3://bucket/_ops/benchmark/ (tuy chon)")
    a = ap.parse_args(argv)

    import boto3

    prefix = f"{a.project}-{a.env}"
    ddb = boto3.resource("dynamodb", region_name=a.region)
    runs = (run_rows(scan(ddb.Table(f"{prefix}-src-job-log")), "src")
            + run_rows(scan(ddb.Table(f"{prefix}-slv-job-log")), "slv"))
    per_job = summarize(runs)
    glue = boto3.client("glue", region_name=a.region)
    jobs = [n for p in glue.get_paginator("get_jobs").paginate() for n in (j["Name"] for j in p["Jobs"])
            if n.startswith(prefix) and n.endswith(SPARK_JOB_SUFFIXES)]
    g = glue_runs(glue, jobs)
    by_t: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)
    for r in g:
        by_t[(r["glue_job"], r["transform"])].append(r)
    glue_sum = [{"glue_job": j, "transform": t, "runs": len(rs), "p50_s": float(statistics.median(r["seconds"] for r in rs)),
                 "dpu_hours": sum(r["dpu_hours"] for r in rs),
                 "usd_est": sum(r["dpu_hours"] for r in rs) * GLUE_USD_PER_DPU_HOUR}
                for (j, t), rs in sorted(by_t.items())]

    today = dt.date.today().isoformat()
    tot = lambda layer, k: sum(r[k] for r in per_job if r["layer"] == layer)  # noqa: E731
    md = (f"# Benchmark pipeline {a.env} ({today})\n\n"
          f"Nguon: job log DynamoDB (lan chay SUCCESS) + Glue get_job_runs. Khong chay them job nao.\n\n"
          f"| Tang | Lan chay | Ban ghi | Thoi gian (s) |\n|---|---|---|---|\n"
          + "".join(f"| {l} | {sum(1 for r in runs if r['layer'] == l)} | {tot(l, 'records'):,.0f} | "
                    f"{tot(l, 'total_s'):,.0f} |\n" for l in ("src", "slv"))
          + f"\n## Theo job (sourcing, silver)\n\n"
          + md_table(per_job, ["layer", "job", "runs", "p50_s", "p95_s", "records", "records_per_s", "mb"])
          + f"\n## Glue Spark (DPU-gio, uoc {GLUE_USD_PER_DPU_HOUR} USD/DPU-gio)\n\n"
          + md_table(glue_sum, ["glue_job", "transform", "runs", "p50_s", "dpu_hours", "usd_est"]))
    out = pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    files = {f"benchmark_{today}.md": md, f"runs_{today}.csv": to_csv(runs),
             f"per_job_{today}.csv": to_csv(per_job), f"glue_runs_{today}.csv": to_csv(g)}
    for n, body in files.items():
        (out / n).write_text(body, encoding="utf-8")
    if a.s3:
        bucket, _, key = a.s3.removeprefix("s3://").partition("/")
        s3 = boto3.client("s3", region_name=a.region)
        for n, body in files.items():
            s3.put_object(Bucket=bucket, Key=f"{key.rstrip('/')}/{today}/{n}", Body=body.encode("utf-8"))
        print(f"Da ghi {len(files)} file len s3://{bucket}/{key.rstrip('/')}/{today}/")
    print(md)
    return 0


if __name__ == "__main__":
    sys.exit(main())
