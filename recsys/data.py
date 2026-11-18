"""Đọc dữ liệu cho recsys qua Athena DƯỚI PERSONA (Lake Formation lọc quyền), tải file kết quả CSV về DataFrame.

Không đọc thẳng Parquet trên S3: sau lf-lockdown mọi truy cập dữ liệu phải qua quyền LF của persona. Kết quả truy vấn
nằm ở nơi ghi của workgroup, persona đọc được file của chính mình. Có cache cục bộ theo băm câu SQL
(recsys/.cache/, không commit) để chạy lại thí nghiệm không tốn Athena.
"""

from __future__ import annotations

import hashlib
import io
import os
import pathlib
import time

CACHE = pathlib.Path(__file__).resolve().parent / ".cache"


def athena_df(sql: str, workgroup: str = "fingraph-dev-data-engineer", profile: str = "fg-de-mcp",
              region: str = "ap-southeast-1", use_cache: bool = True, timeout_s: int = 900):
    import boto3
    import pandas as pd

    key = hashlib.sha256(f"{workgroup}\n{sql}".encode()).hexdigest()[:16]
    path = CACHE / f"{key}.parquet"
    if use_cache and path.exists():
        return pd.read_parquet(path)
    sess = boto3.Session(profile_name=profile or os.environ.get("AWS_PROFILE"))
    ath = sess.client("athena", region_name=region)
    qid = ath.start_query_execution(QueryString=sql, WorkGroup=workgroup)["QueryExecutionId"]
    t0 = time.monotonic()
    while True:
        q = ath.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        st = q["Status"]["State"]
        if st in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if time.monotonic() - t0 > timeout_s:
            ath.stop_query_execution(QueryExecutionId=qid)
            raise TimeoutError(f"Athena qua {timeout_s}s")
        time.sleep(1)
    if st != "SUCCEEDED":
        raise RuntimeError(f"Athena {st}: {q['Status'].get('StateChangeReason', '')[:500]}")
    out = q["ResultConfiguration"]["OutputLocation"]
    bucket, _, k = out.removeprefix("s3://").partition("/")
    body = sess.client("s3", region_name=region).get_object(Bucket=bucket, Key=k)["Body"].read()
    df = pd.read_csv(io.BytesIO(body))
    scanned = q.get("Statistics", {}).get("DataScannedInBytes", 0) / 1e6
    print(f"  athena {qid[:8]}: {len(df):,} dong, quet {scanned:.1f} MB, {time.monotonic() - t0:.0f}s")
    CACHE.mkdir(exist_ok=True)
    df.to_parquet(path, index=False)
    return df
