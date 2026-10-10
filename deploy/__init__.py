"""Đóng gói và deploy code ETL lên nền tảng do repo hạ tầng dựng sẵn.

    deploy/glue_bootstrap.py  script mọi Glue job profile chạy (upload khoá cố định)
    deploy/build.py           đóng gói sourcing/ silver/ orchestrator/ -> etl.zip
    deploy/config_sync.py     entry YAML -> bảng job_config (thay Terraform seed)
    deploy/deploy.py          điều phối: upload -> Lambda -> con trỏ release -> config

Chạy từ CI (OIDC role fingraph-etl-deployer) hoặc từ máy:
    python -m deploy.deploy --env dev --sha $(git rev-parse HEAD)
"""
