"""Điểm vào Glue job gold-spark / pretrain-spark (bootstrap: --ETL_MODULE transforms.main).

    --TRANSFORM   tên logic trong REGISTRY, vd gold.berka_uc2
    --LAYER       gold | pretrain — job nào chỉ chạy logic của tầng đó (job gold không
                  ghi được bảng pre-train dù có ai truyền nhầm --TRANSFORM)
    các --KHOA khác: tham số của từng logic (args.get("SOURCE_DB")...)

Chạy tay:
    aws glue start-job-run --job-name fingraph-dev-gold-spark --arguments '{"--TRANSFORM":"gold.berka_uc2"}'
"""

from __future__ import annotations

import importlib
import os
import sys
import time
from typing import Dict, List, Optional

REGISTRY = {
    "gold.berka_uc2": "transforms.gold.berka_uc2",
    "pretrain.uc2_next_product": "transforms.pretrain.uc2_next_product",
    # Tang ontology (docs/ONTOLOGY-BUILD.md): job onto-spark, --LAYER onto
    "onto.load_schema": "transforms.onto.load_schema",
}


def parse_args(argv: Optional[List[str]] = None) -> Dict[str, str]:
    """`--KHOA value` viết hoa -> dict. Bỏ qua cờ của Glue (--job-bookmark-option...)."""
    argv = sys.argv[1:] if argv is None else argv
    out: Dict[str, str] = {}
    i = 0
    while i < len(argv):
        tok = argv[i]
        if tok.startswith("--") and tok[2:].replace("_", "").isupper() and i + 1 < len(argv):
            out[tok[2:]] = argv[i + 1]
            i += 2
            continue
        i += 1
    return out


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    name = args.get("TRANSFORM")
    if name not in REGISTRY:
        raise SystemExit(f"--TRANSFORM {name!r} khong co. Co: {sorted(REGISTRY)}")
    layer = args.get("LAYER")
    if layer and not name.startswith(layer + "."):
        raise SystemExit(f"Job tang {layer!r} khong chay {name!r}")

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(name).getOrCreate()
    t0 = time.monotonic()
    print(f"[{name}] ETL_RELEASE={os.environ.get('ETL_RELEASE', '?')} args={args}")
    result = importlib.import_module(REGISTRY[name]).run(spark, args)
    print(f"[{name}] XONG sau {time.monotonic() - t0:.1f}s: {result}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
