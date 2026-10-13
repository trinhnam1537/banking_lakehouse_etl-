"""Chạy MỘT entry sourcing trên stack local, bằng đúng framework chạy trên Glue.

    python -m local.run_entry corebank_trans
    python -m local.run_entry corebank_trans --override jdbc.chunk_size=1000
    python -m local.run_entry aml_transfers --show 2      # in 2 dòng raw đầu tiên
    python -m local.run_entry vcb_exchange_rates --reset   # xoá checkpoint, kéo lại từ đầu

Khác chạy trên Glue đúng hai chỗ, đều có chủ đích:
  * enabled ép thành true: entry dùng profile src-vpc đang tắt chờ repo hạ tầng,
    nhưng trên máy thì Postgres/Kafka nằm ngay localhost.
  * runtime.profile ép thành src-shell (máy local có đủ thư viện).
Còn lại — checkpoint, writer, manifest, CDC — là code thật, ghi vào moto.
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys

from . import env  # noqa: F401

import os

import boto3
import yaml

from sourcing.config import apply_overrides, from_dict
from sourcing.runner import SourceRunner
from sourcing.state import JobLogStore

from deploy.build import REPO


def find_entry(name: str):
    hits = list((REPO / "sourcing_etl" / "sources").rglob(f"{name}.yml"))
    if not hits:
        raise SystemExit(f"Khong thay sourcing_etl/sources/**/{name}.yml")
    return hits[0]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("job")
    ap.add_argument("--override", action="append", default=[], help="khoa.cham=gia_tri_json")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--reset", action="store_true", help="xoa checkpoint truoc khi chay")
    ap.add_argument("--show", type=int, default=0, help="in N dong raw dau tien sau khi chay")
    args = ap.parse_args(argv)

    with open(find_entry(args.job), encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    raw["enabled"] = True
    raw.setdefault("runtime", {})["profile"] = "src-shell"
    overrides = {}
    for o in args.override:
        k, _, v = o.partition("=")
        try:
            overrides[k] = json.loads(v)
        except ValueError:
            overrides[k] = v
    cfg = from_dict(apply_overrides(raw, overrides) if overrides else raw)

    table = os.environ["JOBLOG_TABLE"]
    if args.reset:
        JobLogStore(table).reset_checkpoint(cfg.name)
        print(f"Da xoa checkpoint {cfg.name}")
    res = SourceRunner(cfg, os.environ["RAW_BUCKET"], table, dry_run=args.dry_run).run()
    print(json.dumps({"status": res.status, "records": res.stats.records_written,
                      "files": res.stats.files_written, "watermarks": res.watermarks}, ensure_ascii=False))

    if args.show and res.stats.files_written:
        s3 = boto3.client("s3")
        prefix = f"{cfg.raw_path}/"
        keys = sorted(o["Key"] for o in s3.list_objects_v2(Bucket=os.environ["RAW_BUCKET"],
                                                           Prefix=prefix).get("Contents", []))
        body = gzip.decompress(s3.get_object(Bucket=os.environ["RAW_BUCKET"], Key=keys[-1])["Body"].read())
        for line in body.decode("utf-8").splitlines()[: args.show]:
            print(json.dumps(json.loads(line), ensure_ascii=False)[:600])
    return 0 if res.ok else 1


if __name__ == "__main__":
    sys.exit(main())
