"""Biến môi trường của stack local. Import module này TRƯỚC boto3.

Không ghi đè biến đã đặt: muốn trỏ sang AWS thật thì đặt biến trước khi chạy.
"""

import os

LOCAL = {
    "AWS_ENDPOINT_URL": "http://localhost:5050",     # moto: S3 + DynamoDB + Secrets Manager
    "AWS_ACCESS_KEY_ID": "local",
    "AWS_SECRET_ACCESS_KEY": "local",
    "AWS_DEFAULT_REGION": "ap-southeast-1",
    "RAW_BUCKET": "fingraph-local-lakehouse",
    "JOBLOG_TABLE": "fingraph-local-src-job-log",
    "SLV_JOBLOG_TABLE": "fingraph-local-slv-job-log",
    "KAFKA_BOOTSTRAP": "localhost:19092",
}

for k, v in LOCAL.items():
    os.environ.setdefault(k, v)
os.environ.pop("AWS_PROFILE", None)  # khong de profile that tren may chen vao
