# banking_lakehouse_etl — code ETL của FinGraph

Repo này chứa **logic**: framework, cấu hình job, script Glue, test, và script deploy
code. Repo này **không tạo resource AWS nào**. Hạ tầng (Glue job, bảng, Lambda, IAM)
nằm ở repo `trinhnam1537/lakehouse_ontology_project`, được dựng sẵn như một **nền tảng**.
Repo này chỉ đổ **code** và **cấu hình** vào nền tảng đó.

| Thư mục | Nội dung |
|---|---|
| `sourcing/` | Framework kéo nguồn (API, JDBC, Kafka, file S3) xuống `landing/` |
| `sourcing_etl/sources/` | Entry YAML, mỗi file một nguồn/bảng |
| `silver/`, `silver_etl/` | Framework raw → silver (Iceberg, SCD2/SCD4a), entry và DDL |
| `orchestrator/` | Lambda trigger (một cửa vào theo `job_id`), Lambda schedule-sync, event mẫu |
| `deploy/` | `glue_bootstrap.py` (script mọi Glue job chạy), `build.py`, `config_sync.py`, `deploy.py` |
| `jobs/glue/` | Script PySpark: bronze_ingest, onto_closure (CI upload lên `_ops/glue-scripts/`) |
| `simulators/` | Hệ thống nguồn giả lập: seed + simulator cho core banking, cho vay, IBM AML, PaySim, thẻ (không deploy) |
| `local/` | Stack local (Postgres, Redpanda, moto), `bootstrap`, `run_entry` chạy một entry bằng framework thật |
| `tests/` | pytest + moto, không chạm AWS |
| `docs/` | Kiến trúc sourcing/silver, tài liệu từng nguồn (`docs/sourcing/`) |

## Chạy thế nào trên AWS

Glue job **không còn tạo theo từng entry**. Repo hạ tầng tạo sẵn vài **Glue job dùng
chung theo profile**: `src-shell` (Python Shell, có internet, có PyMySQL) và
`slv-spark` (Spark). Mỗi lần chạy, Lambda trigger truyền tham số để chọn entry và
phiên bản code:

```
Lambda trigger ── job_id ──▶ đọc job_config, chọn profile theo runtime.profile
                              ├─ --JOB_NAME, --CONFIG_SNAPSHOT   (chạy entry nào)
                              ├─ --ETL_RELEASE = <sha>           (bản code nào)
                              └─ Timeout, khoá LOCK theo entry   (Step Functions)
Glue job src-shell ──▶ deploy/glue_bootstrap.py (khoá S3 cố định)
                        tải s3://…/_ops/etl/releases/<sha>/etl.zip ─▶ sourcing.main
```

**Thêm một nguồn = thêm một file YAML rồi merge.** Không có Terraform nào chạy.

## Deploy

```
push main ─▶ CI: pytest 3.9 + 3.12 ─▶ python -m deploy.deploy --env dev --sha <commit>
              1. build etl.zip ─▶ s3://<bucket>/_ops/etl/releases/<sha>/etl.zip
              2. bootstrap     ─▶ s3://<bucket>/_ops/etl/bootstrap/glue_bootstrap.py
                 DDL silver    ─▶ s3://<bucket>/_ops/silver/ddl/
              3. update-function-code: trigger, schedule-sync
                 script Glue    ─▶ s3://<bucket>/_ops/glue-scripts/*.py
              4. SSM /fingraph/dev/etl/release = <sha>          (con trỏ phiên bản)
              5. config-sync: YAML ─▶ bảng job_config (src, slv)
```

- Code lên **trước**, cấu hình **sau**: cấu hình mới có thể cần code mới, còn code mới
  vẫn đọc được cấu hình cũ.
- Mỗi lần chạy Glue **ghim một SHA**. Deploy đúng lúc job đang chạy không làm đổi code
  của lần chạy đó.
- **Rollback:** chạy lại workflow CI trên commit cũ, hoặc
  `python -m deploy.deploy --env dev --sha <sha-cũ>`.
- Entry bị xoá khỏi repo thì `config-sync` đặt `enabled = false`, không xoá item.

Quyền AWS: OIDC role **`fingraph-etl-deployer`** (tạo ở `terraform/bootstrap` của repo hạ
tầng), chỉ tin nhánh `main` của repo này. Role chỉ có quyền ghi `_ops/etl/*`, ghi bảng
`*-job-config`, cập nhật code đúng ba Lambda, và ghi SSM `/fingraph/<env>/etl/*`. **Không
có quyền IAM, không tạo hay xoá resource.** Repository variable cần đặt:
`AWS_DEPLOY_ROLE_ARN`.

## Hợp đồng với repo hạ tầng

Repo này chỉ biết nền tảng qua SSM **`/fingraph/<env>/platform/*`**. Repo hạ tầng ghi
các tham số này (`terraform/envs/<env>/platform.tf`):

| Tham số | Dùng để |
|---|---|
| `artifact_bucket` | Nơi upload etl.zip, bootstrap, DDL |
| `src_config_table`, `slv_config_table` | Đích của config-sync |
| `lambda_functions` | Các hàm cần update-function-code |
| `release_param` | Tên SSM con trỏ phiên bản |
| `profile_jobs` | Profile → Glue job (tham khảo) |

Phía repo này phải giữ ổn định:
- Khoá S3 `_ops/etl/bootstrap/glue_bootstrap.py` và `_ops/etl/releases/<sha>/etl.zip`.
- Handler Lambda: `orchestrator.trigger.handler`, `orchestrator.schedule_sync.handler`.
- Khoá S3 `_ops/glue-scripts/<tên>.py` của script trong `jobs/glue/`.
- Tên profile trong `sourcing/config.py` (`PROFILES`) phải khớp `var.profiles` của repo hạ tầng.
- Secret của nguồn phải nằm dưới `fingraph/src/` (`SECRET_PREFIX`), vì Glue chỉ đọc được tiền tố này.

**Thêm profile mới** (thư viện mới như `confluent-kafka`, hoặc mạng mới như RDS private):
thêm ở repo hạ tầng **trước** (`var.profiles`), apply xong mới thêm tên vào `PROFILES` ở đây.

## Chạy trên máy

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m pytest -q

# Thu mot job, khong can AWS: goi nguon that, dem ban ghi
.\scripts\run-source.ps1 -Job berka_order -Mode dry

# Xem deploy se lam gi, khong ghi gi (can credential doc SSM + DynamoDB)
python -m deploy.deploy --env dev --sha $(git rev-parse HEAD) --dry-run
```

Code chạy trên Glue Python Shell phải tương thích **Python 3.9**.
