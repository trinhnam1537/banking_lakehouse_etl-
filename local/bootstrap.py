"""Dựng tài nguyên "AWS" trên stack local (moto) + topic Kafka. Chạy lại bao nhiêu lần cũng được.

    docker compose -f local/docker-compose.yml up -d
    python -m local.bootstrap

Tạo:
  S3        fingraph-local-lakehouse           (landing/, landing/_inbox/)
  DynamoDB  fingraph-local-src-job-log, fingraph-local-slv-job-log (cùng key schema với repo hạ tầng)
  Secrets   fingraph/src/corebank_pg, fingraph/src/lending_pg   (user etl_reader CHỈ ĐỌC)
            fingraph/src/berka_ctu                               (guest công khai của CTU)
  Kafka     aml.interbank.transfers (6 partition), card.authorizations (6 partition)

moto giữ dữ liệu trong RAM: khởi động lại container moto là mất, chạy lại script này.
"""

from __future__ import annotations

import json
import sys

from . import env  # noqa: F401  (dat bien moi truong truoc boto3)

import os

import boto3

SECRETS = {
    "fingraph/src/corebank_pg": {"host": "localhost", "port": 5433, "username": "etl_reader",
                                 "password": "etl_reader", "dbname": "corebank"},
    "fingraph/src/lending_pg": {"host": "localhost", "port": 5434, "username": "etl_reader",
                                "password": "etl_reader", "dbname": "lending"},
    "fingraph/src/berka_ctu": {"host": "relational.fel.cvut.cz", "port": 3306, "username": "guest",
                               "password": "ctu-relational", "dbname": "financial"},
}
TOPICS = {"aml.interbank.transfers": 6, "card.authorizations": 6}


def main() -> int:
    bucket, table = os.environ["RAW_BUCKET"], os.environ["JOBLOG_TABLE"]
    s3 = boto3.client("s3")
    try:
        s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": "ap-southeast-1"})
        print(f"S3 {bucket}: tao moi")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"S3 {bucket}: da co")

    ddb = boto3.client("dynamodb")
    for t in (table, os.environ["SLV_JOBLOG_TABLE"]):   # job_log cua sourcing va silver
        if t not in ddb.list_tables()["TableNames"]:
            ddb.create_table(
                TableName=t, BillingMode="PAY_PER_REQUEST",
                KeySchema=[{"AttributeName": "job_name", "KeyType": "HASH"},
                           {"AttributeName": "run_key", "KeyType": "RANGE"}],
                AttributeDefinitions=[{"AttributeName": "job_name", "AttributeType": "S"},
                                      {"AttributeName": "run_key", "AttributeType": "S"}])
            print(f"DynamoDB {t}: tao moi")
        else:
            print(f"DynamoDB {t}: da co")

    sm = boto3.client("secretsmanager")
    for name, value in SECRETS.items():
        try:
            sm.create_secret(Name=name, SecretString=json.dumps(value))
        except sm.exceptions.ResourceExistsException:
            sm.put_secret_value(SecretId=name, SecretString=json.dumps(value))
        print(f"Secret {name}")

    try:
        from confluent_kafka.admin import AdminClient, NewTopic

        admin = AdminClient({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"]})
        existing = set(admin.list_topics(timeout=10).topics)
        new = [NewTopic(t, num_partitions=p, replication_factor=1,
                        config={"retention.ms": str(14 * 86400 * 1000)})
               for t, p in TOPICS.items() if t not in existing]
        for t, f in admin.create_topics(new).items() if new else []:
            f.result()
            print(f"Kafka topic {t}: tao moi")
        for t in TOPICS:
            if t in existing:
                print(f"Kafka topic {t}: da co")
    except Exception as e:  # noqa: BLE001
        print(f"Kafka: bo qua ({e})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
