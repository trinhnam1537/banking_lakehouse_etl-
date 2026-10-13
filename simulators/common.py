"""Dùng chung cho mọi simulator: kết nối, đồng hồ nghiệp vụ, ghi inbox S3, nhãn trễ.

Mọi tham số lấy từ biến môi trường, mặc định trỏ vào stack local
(`local/docker-compose.yml`). Chạy trên EC2 "hệ thống nguồn" thì chỉ đổi biến.

    COREBANK_DSN     postgresql://corebank:corebank@localhost:5433/corebank
    LENDING_DSN      postgresql://lending:lending@localhost:5434/lending
    KAFKA_BOOTSTRAP  localhost:19092
    RAW_BUCKET       bucket chứa landing/_inbox/ (file EOD, nhãn)
    AWS_ENDPOINT_URL http://localhost:5050 khi dùng moto (bỏ trống trên AWS)
    SIM_EPOCH        2023-01-01 — ngày nghiệp vụ đầu tiên sau dữ liệu lịch sử
    DATA_DIR         ./data — nơi đặt file Kaggle đã tải (bị .gitignore)

ĐỒNG HỒ CHUNG. Bảy nguồn đến từ bảy thời kỳ khác nhau (Berka 1993-98, IBM AML
09/2022, Sparkov 2019-20, PaySim không có ngày...). Simulator dời tất cả về một
trục: lịch sử kết thúc trước SIM_EPOCH, dữ liệu mới sinh từ SIM_EPOCH trở đi.
Tỷ giá VCB có từ giữa 2020 nên trục này quy đổi được sang VND.

NHÃN TRỄ. Nhãn gian lận/rửa tiền/vỡ nợ KHÔNG đi cùng giao dịch: ngân hàng chỉ
biết sau điều tra hoặc chargeback. `LabelSink` giữ nhãn lại N ngày nghiệp vụ rồi
mới ghi thành file riêng — demo phát hiện thời gian thực không bị rò nhãn.
"""

from __future__ import annotations

import csv
import datetime as dt
import gzip
import hashlib
import io
import json
import os
import pathlib
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Sequence

REPO = pathlib.Path(__file__).resolve().parent.parent

# SIM_TARGET=local (mặc định): dùng biến của stack local (moto, localhost) cho những
# gì chưa đặt. Trên máy nguồn ở AWS đặt SIM_TARGET=aws để boto3 dùng S3 thật.
if os.environ.get("SIM_TARGET", "local") == "local":
    import local.env  # noqa: F401,E402


def env(name: str, default: str) -> str:
    return os.environ.get(name) or default


SIM_EPOCH = dt.date.fromisoformat(env("SIM_EPOCH", "2023-01-01"))
DATA_DIR = pathlib.Path(env("DATA_DIR", str(REPO / "data")))
INBOX_ROOT = "landing/_inbox"


def pg(dsn_env: str, default: str):
    import psycopg2

    conn = psycopg2.connect(env(dsn_env, default))
    conn.autocommit = False
    return conn


def _dsn_from_secret(secret_id: str, host_override: Optional[str]) -> str:
    """DSN từ secret {host, port, username, password, dbname, sslmode} (máy nguồn trên AWS)."""
    import boto3

    s = json.loads(boto3.client("secretsmanager").get_secret_value(SecretId=secret_id)["SecretString"])
    return (f"host={host_override or s['host']} port={s.get('port', 5432)} dbname={s['dbname']} "
            f"user={s['username']} password={s['password']} sslmode={s.get('sslmode', 'prefer')}")


def corebank_conn():
    """Local: COREBANK_DSN (mặc định stack docker). AWS: COREBANK_SECRET (+ COREBANK_HOST)."""
    secret = os.environ.get("COREBANK_SECRET")
    if secret:
        import psycopg2

        conn = psycopg2.connect(_dsn_from_secret(secret, os.environ.get("COREBANK_HOST")))
        conn.autocommit = False
        return conn
    return pg("COREBANK_DSN", "postgresql://corebank:corebank@localhost:5433/corebank")


def lending_conn():
    return pg("LENDING_DSN", "postgresql://lending:lending@localhost:5434/lending")


def kafka_producer(client_id: str):
    """Producer an toàn: idempotent + acks=all — producer thử lại không sinh bản trùng."""
    from confluent_kafka import Producer

    return Producer({
        "bootstrap.servers": env("KAFKA_BOOTSTRAP", "localhost:19092"),
        "client.id": client_id,
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "gzip",
        "linger.ms": 50,
        "batch.num.messages": 10000,
    })


def s3():
    import boto3

    return boto3.client("s3")  # boto3 >= 1.28 tu doc AWS_ENDPOINT_URL


def inbox_bucket() -> str:
    b = os.environ.get("RAW_BUCKET")
    if not b:
        raise SystemExit("Thieu RAW_BUCKET (bucket chua landing/_inbox/). Local: fingraph-local-lakehouse")
    return b


def inbox_key(source: str, kind: str, day: dt.date, filename: str) -> str:
    # dt=YYYY-MM-DD: reader s3file lay KHOA S3 lam moc va list theo thu tu tu dien
    # -> file ngay sau luon dung sau file ngay truoc.
    return f"{INBOX_ROOT}/{source}/{kind}/dt={day.isoformat()}/{filename}"


def csv_gz(header: Sequence[str], rows: Iterable[Sequence[Any]]) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(header)
    w.writerows(rows)
    return gzip.compress(buf.getvalue().encode("utf-8"), mtime=0)


def ndjson_gz(rows: Iterable[Dict[str, Any]]) -> bytes:
    body = "".join(json.dumps(r, ensure_ascii=False, default=str) + "\n" for r in rows)
    return gzip.compress(body.encode("utf-8"), mtime=0)


def put_inbox(key: str, body: bytes, dry: bool = False) -> None:
    if dry:
        print(f"  [dry] s3://<bucket>/{key} ({len(body)} bytes)")
        return
    s3().put_object(Bucket=inbox_bucket(), Key=key, Body=body)
    print(f"  -> s3://{inbox_bucket()}/{key} ({len(body)} bytes)")


def stable_id(*parts: Any, n: int = 20) -> str:
    """Mã tất định: chạy lại simulator cho cùng mã -> silver khử trùng được."""
    return hashlib.sha1("|".join(str(p) for p in parts).encode()).hexdigest()[:n]


class LabelSink:
    """Gom nhãn theo ngày nghiệp vụ, ghi file sau `delay_days` ngày.

    Gọi `advance(today)` mỗi khi đồng hồ sang ngày mới; `close()` xả phần còn lại
    (khi phát lại xong toàn bộ file).
    """

    def __init__(self, source: str, header: Sequence[str], delay_days: int, dry: bool = False):
        self.source, self.header, self.delay, self.dry = source, list(header), delay_days, dry
        self.pending: Dict[dt.date, List[Sequence[Any]]] = defaultdict(list)

    def add(self, day: dt.date, row: Sequence[Any]) -> None:
        self.pending[day].append(row)

    def advance(self, today: dt.date) -> None:
        for day in sorted(d for d in self.pending if d <= today - dt.timedelta(days=self.delay)):
            self._flush(day)

    def close(self) -> None:
        for day in sorted(self.pending):
            self._flush(day)

    def _flush(self, day: dt.date) -> None:
        rows = self.pending.pop(day)
        key = inbox_key(self.source, "labels", day, f"labels_{day:%Y%m%d}.csv.gz")
        put_inbox(key, csv_gz(self.header, rows), self.dry)


def find_file(*candidates: str) -> Optional[pathlib.Path]:
    for c in candidates:
        p = DATA_DIR / c
        if p.exists():
            return p
    return None
