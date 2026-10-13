# Kiến trúc sourcing — thiết kế, triển khai, vận hành

Tầng **sourcing** kéo dữ liệu từ nguồn bên ngoài (REST API, Postgres, Kafka, file S3)
xuống tầng raw `landing/` của lakehouse, rồi báo cho luồng silver chạy tiếp.

Tài liệu này mô tả **toàn cảnh**: các thành phần, cách code đi từ máy lên AWS, cách
một job được kích và chạy, dữ liệu nằm ở đâu, cách test, cách xử lý sự cố. Chi tiết
từng phần có ở nơi khác và được dẫn link.

| Muốn biết | Đọc |
|---|---|
| Cách viết một entry YAML | [sourcing_etl/README.md](../sourcing_etl/README.md) |
| Bên trong framework Python | [sourcing/README.md](../sourcing/README.md) |
| Lambda trigger, event mẫu | [orchestrator/README.md](../orchestrator/README.md) |
| Tầng sau: raw → silver | [SILVER-ARCHITECTURE.md](SILVER-ARCHITECTURE.md) |
| Lệnh đã chạy, lỗi đã gặp | `docs/SETUP-LOG.md` của **repo hạ tầng** (`lakehouse_ontology_project`) |
| Terraform, IAM, CI deploy | **repo hạ tầng**. Repo này không tạo resource AWS nào |

> **Trạng thái (09-28).** Framework và 59 Glue job đã chạy trên dev (lần chạy thành
> công đầu tiên: `fdic_institutions`, 09-28). Orchestrator (mục 4) đã viết xong và
> có test, nhưng **chưa apply**. Lịch của mọi job đang **tắt**.

---

## Mục lục

1. [Tổng quan](#1-tổng-quan)
2. [Thành phần](#2-thành-phần)
3. [Từ code đến AWS](#3-từ-code-đến-aws)
4. [Một lần chạy đi qua những đâu](#4-một-lần-chạy-đi-qua-những-đâu)
5. [Bên trong Glue job](#5-bên-trong-glue-job)
6. [Dữ liệu và trạng thái](#6-dữ-liệu-và-trạng-thái)
7. [Lập lịch](#7-lập-lịch)
8. [Chạy và kiểm thử](#8-chạy-và-kiểm-thử)
9. [Vận hành và xử lý sự cố](#9-vận-hành-và-xử-lý-sự-cố)
10. [Thêm một nguồn mới](#10-thêm-một-nguồn-mới)
11. [Phân quyền](#11-phân-quyền)
12. [Chi phí](#12-chi-phí)
13. [Quyết định thiết kế](#13-quyết-định-thiết-kế)
14. [Hạn chế và việc tiếp theo](#14-hạn-chế-và-việc-tiếp-theo)

---

## 1. Tổng quan

```
 NGUỒN                    ĐIỀU PHỐI                         CHẠY                     ĐÍCH
 ─────                    ─────────                         ────                     ────
 REST API (56 entry)                                                           s3://fingraph-lakehouse-dev-*/
 Postgres  (1)         Lịch / gọi tay / luồng trước                             landing/<dataset>/ingest_date=…/
 Kafka     (1)    ──▶  Lambda trigger ──▶ Step Functions ──▶ Glue Python Shell ──▶   part-*.ndjson.gz
 File S3   (1)                                  job-runner       (framework         landing/_manifests/…/<run_id>.json
                                                    │             sourcing)                    │
                                                    └── "Job Run Succeeded" ──▶ silver ◀───────┘
```

Ba nguyên tắc định hình mọi thứ còn lại:

1. **Thêm một nguồn là thêm một file YAML, không viết Python.** Terraform đọc YAML để
   sinh Glue job và seed bảng cấu hình. Framework đọc cùng cấu hình đó lúc chạy.
2. **Raw không áp schema.** Mỗi HTTP response được ghi nguyên văn thành một dòng
   NDJSON gzip. Ép kiểu là việc của silver.
3. **At-least-once, không bao giờ mất dữ liệu.** Dữ liệu được ghi trước, checkpoint
   mới tiến sau. Phần chồng lấn khi chạy lại do silver khử trùng theo khoá nghiệp vụ.

Hiện có **37 entry từ 8 nhóm nguồn** của ngân hàng giả lập: Berka, core banking, cho vay
(Home Credit), IBM AML, PaySim, thẻ, OpenSanctions và tỷ giá (VCB, CNB). Danh mục, schema,
credential và luồng giả lập của từng nguồn: [docs/sourcing/](sourcing/README.md). (59 entry của
10 nguồn tài chính công khai cũ, như SEC, GLEIF hay FDIC, đã được gỡ ở commit `4d70a79`.)

---

## 2. Thành phần

### Trong repo

```
sourcing/                  FRAMEWORK — code chạy trong Glue job
  config.py                  schema entry, kiểm tra, override, snapshot
  readers/                   api · jdbc · kafka · s3file · httpfile — mọi khác biệt nguồn nằm sau read()
                               fileparse.py: đọc CSV/NDJSON/JSON (gzip) THEO LUỒNG
  sqltemplate.py             SQL trong entry JDBC: tham số :watermark/:chunk_size, kiểm tra tĩnh
  writer.py                  NDJSON gzip -> landing/, manifest
  runner.py                  luồng chung: checkpoint -> đọc -> ghi -> tiến mốc
  state.py                   JobConfigStore, JobLogStore (checkpoint + nhật ký + ORCH#)
  main.py                    điểm vào (Glue và chạy tay)
  lambda_dispatcher.py       [CŨ] Lambda fan-out theo lịch 5 phút

sourcing_etl/
  sources/<nguồn>/<job>.yml  CẤU HÌNH — mỗi file một job, tên file = tên job
  glue_entrypoint.py         script Glue thực sự chạy, gọi sourcing.main

orchestrator/              [MỚI] một cửa vào cho mọi luồng
  trigger.py                 Lambda trigger
  job_types.py               src (sourcing), slv (silver)
  events/                    12 event mẫu để test

tests/sourcing/  tests/orchestrator/     pytest + moto, không chạm AWS
```

Hạ tầng tương ứng nằm ở **repo hạ tầng**: `terraform/modules/sourcing` (Glue job cho mỗi
entry, `job_config`, `job_log`, chuỗi điều phối cũ) và `terraform/modules/orchestrator`
(Lambda trigger, SFN job-runner, rule, lịch cho từng job).

### Trên AWS (dev, `ap-southeast-1`)

| Resource | Tên | Vai trò |
|---|---|---|
| Glue job × 51 | `fingraph-dev-src-<job>` | Mỗi entry đang bật một job, Python Shell 0.0625 DPU, `max_concurrent_runs = 1` |
| S3 | `_ops/sourcing/sourcing.zip`, `glue_entrypoint.py` | Code framework cho Glue |
| DynamoDB | `fingraph-dev-src-job-config` | Cấu hình lúc chạy, seed từ YAML |
| DynamoDB | `fingraph-dev-src-job-log` | Nhật ký chạy, checkpoint, `ORCH#` (TTL 90 ngày, riêng checkpoint hiện hành thì không) |
| Lambda | `fingraph-dev-trigger` | **[mới]** Cửa vào: nhận `job_id`, kích SFN |
| Step Functions | `fingraph-dev-job-runner` | **[mới]** Chạy Glue `.sync`, ghi kết quả, báo luồng sau |
| EventBridge rule | `fingraph-dev-job-succeeded` | **[mới]** Sourcing xong thì gọi lại Lambda trigger để kích silver |
| Scheduler group | `fingraph-dev-jobs` | **[mới]** Mỗi job một lịch, đang tắt |
| Scheduler, 2 SQS FIFO + 2 DLQ, Lambda, Pipes, SFN | `fingraph-dev-src-*` | **[cũ]** Chuỗi dispatcher, sẽ gỡ sau khi luồng mới ổn định |
| EventBridge rule | `fingraph-dev-glue-job-failed` | Glue FAILED/TIMEOUT thì gửi SNS `fingraph-dev-alerts` (module `ops`) |

---

## 3. Từ code đến AWS

Code và hạ tầng nằm ở **hai repo**. Repo này (ETL) không tạo resource nào. Repo hạ tầng
ghim một commit của repo này trong `etl.ref`, rồi Terraform đọc code tại đúng commit đó.

```
 REPO ETL (repo này)                    REPO HẠ TẦNG (lakehouse_ontology_project)        AWS
 ───────────────────                    ─────────────────────────────────────────        ───
 sửa YAML / Python
 pytest (304 test), run-source.ps1 -Mode dry
        │ PR
        ▼
 tests.yml ── pytest 3.9 (Glue) + 3.12 (Lambda)
        │ merge main            (CHƯA deploy gì)
        ▼
                                PR: etl.ref = <SHA mới>
                                terraform-plan.yml
                                  · checkout repo ETL đúng SHA vào etl/ (deploy key chỉ đọc)
                                  · fmt, validate, plan dev + prd -> comment vào PR
                                        │ merge
                                        ▼
                                terraform-apply.yml ── BẤM TAY, gõ APPLY, OIDC
                                        │
                                        ▼ terraform apply envs/dev
          ┌────────────────────────────────────────────────────────────────────────────┐
          │ etl/sourcing/*.py           ──zip──▶ S3 _ops/sourcing/sourcing.zip  (Glue)  │
          │ etl/orchestrator + sourcing                                                 │
          │   + silver/*.py             ──zip──▶ Lambda fingraph-dev-trigger            │
          │ etl/sourcing_etl/**/*.yml   ──────▶ 1 aws_glue_job cho mỗi entry bật        │
          │                             ──────▶ 1 item cho mỗi entry trong job_config   │
          │                             ──────▶ quyền đọc đúng các secret entry khai    │
          │                             ──────▶ 1 lịch cho mỗi entry có schedule.enabled│
          │                                     (chỉ khi use_orchestrator_schedules=true)│
          └────────────────────────────────────────────────────────────────────────────┘
```

**Hệ quả cần nhớ:** sửa YAML hoặc sửa Python ở đây **chưa** có hiệu lực trên AWS. Phải
bump `etl.ref` ở repo hạ tầng rồi apply. Rollback là đặt `etl.ref` về SHA cũ. Riêng khi
test thì không cần deploy: dùng `overrides` trong event (mục 8) để đổi tham số cho một lần
chạy.

**Hợp đồng giữa hai repo:** Terraform đọc các đường dẫn liệt kê trong `README.md` của repo
này. Đổi tên hoặc chuyển chỗ một đường dẫn trong đó là breaking change cho repo hạ tầng.

Glue Python Shell chạy **Python 3.9**, nên code trong `sourcing/` không được dùng cú pháp
của 3.10 trở lên (`match`, `X | Y`). CI test cả hai phiên bản.

---

## 4. Một lần chạy đi qua những đâu

### 4.1 Luồng mới (orchestrator)

```
 ① Nguồn kích
    EventBridge Schedule  {"job_id":"src.gleif_lei","trigger":{"type":"schedule","scheduled_time":…}}
    Gọi tay               {"job_id":"src.gleif_lei","wait_seconds":600}
    Luồng trước xong      event "Job Run Succeeded"
            │
            ▼
 ② Lambda fingraph-dev-trigger
    · job_id -> loại (src/slv) + tên
    · GetItem job_config — MỘT lần; job tắt -> SKIPPED
    · áp overrides, kiểm tra bằng from_dict()        (sai -> trả lỗi, không kích gì)
    · run_id: từ lịch = hash(job, giờ lịch); gọi tay = ngẫu nhiên
    · PutItem ORCH#<run_id> CÓ ĐIỀU KIỆN             (đã có -> DUPLICATE)
    · StartExecution name=src-<job>-<run_id>
    · wait_seconds > 0: DescribeExecution mỗi 10s, trả tóm tắt RUN# + ORCH#
            │
            ▼
 ③ Step Functions fingraph-dev-job-runner
    RunGlueJob  glue:startJobRun.sync
                 --JOB_NAME --RUN_ID --DRY_RUN --CONFIG_SNAPSHOT
                 retry: ConcurrentRunsExceeded (đợi tối đa ~3,6 giờ), throttling
      ├─ OK    ▶ RecordSuccess  ORCH# orch_status=SUCCEEDED, đọc lại manifest
      │          ▶ ShouldChain  chain=true VÀ có manifest?
      │              ├─ có    ▶ EmitSucceeded -> EventBridge "Job Run Succeeded"
      │              └─ không ▶ Done       (dry-run, NO_DATA, có override)
      └─ lỗi   ▶ RecordFailure  ORCH# orch_status=FAILED, err=<lỗi>  ▶ Fail
            │
            ▼
 ④ Glue job fingraph-dev-src-<job>  (mục 5)
    ghi landing/ + manifest, tiến checkpoint, ghi RUN#, ghi manifest vào ORCH#
            │
            ▼
 ⑤ Rule fingraph-dev-job-succeeded ▶ Lambda trigger (lần 2)
    · tìm entry silver đang bật có source.dataset = dataset của manifest
    · claim TRIG#<manifest> (một manifest chỉ được xử lý một lần) ▶ StartExecution slv-<job>-…
```

Mọi luồng dùng chung một state machine. Lambda đã dựng sẵn tên Glue job và tham số, nên
thêm luồng gold chỉ cần thêm một lớp trong `orchestrator/job_types.py`.

### 4.2 Luồng cũ (vẫn còn trên AWS)

```
Scheduler rate(5 min) ▶ SQS dispatch.fifo ▶ Lambda dispatcher ▶ SQS run.fifo ▶ Pipes ▶ SFN src-runner ▶ Glue
                                          (tự parse cron, so với
                                           last_dispatched_at)
```

Hai luồng không chạy đè lên nhau nhờ **một công tắc** `use_orchestrator_schedules` trong
`terraform/envs/dev/main.tf` của repo hạ tầng:

| `use_orchestrator_schedules` | Lịch 5 phút của dispatcher | Lịch riêng cho từng job |
|---|---|---|
| `false` (hiện tại) | ENABLED, nhưng mọi entry có `schedule.enabled: false` nên không bắn job nào | không tạo |
| `true` | **DISABLED** | tạo cho mỗi entry có `schedule.enabled: true` |

Gọi tay qua Lambda trigger chạy được trong **cả hai** trạng thái.

### 4.3 Vì sao thiết kế như vậy (so với luồng cũ)

| Luồng cũ | Luồng mới | Lý do |
|---|---|---|
| SQS giữa Scheduler và Lambda | Scheduler/EventBridge gọi Lambda trực tiếp | Gọi bất đồng bộ đã có retry và giới hạn tuổi event |
| SQS FIFO + Pipes trước SFN | Lambda gọi `StartExecution` | Pipes `FIRE_AND_FORGET` xoá message ngay, nên SQS không đệm được gì cho Glue |
| Dedup của FIFO theo phút | `ORCH#` có điều kiện + tên execution duy nhất | Chặn trùng theo đúng tick lịch, không phụ thuộc cửa sổ 5 phút |
| Cron parser tự viết | Mỗi job một EventBridge Schedule | Cron đầy đủ, không phải tự bảo trì |
| `last_dispatched_at` ghi vào item Terraform quản lý | Trạng thái chạy nằm trong `job_log` | Apply lại không xoá trạng thái, nên không kích mọi job cùng lúc |
| Silver nghe S3 event rồi đoán ngược | SFN bắn event có sẵn `job_id`, `run_id`, `manifest` | Nơi biết chắc job xong mới là nơi báo |
| Không có bước "prepare/release resource" | — | Glue serverless không có cụm để dựng. Khoá đã có sẵn ở `max_concurrent_runs = 1` |

---

## 5. Bên trong Glue job

[sourcing/runner.py](../sourcing/runner.py), điểm vào là [sourcing/main.py](../sourcing/main.py).

```
main.py
  cấu hình, theo thứ tự ưu tiên:
    --config file.yml        chạy tay trên máy
    --CONFIG_SNAPSHOT        ảnh chụp Lambda đã đọc + override + kiểm tra   ◀ luồng mới
    --JOB_CONFIG_TABLE       tự đọc DynamoDB                                 ◀ luồng cũ
  enabled=false -> thoát 0

runner.run()
  RUN#<iso>#<run_id>  status=RUNNING                 (ghi NGAY, job bị giết vẫn còn dấu vết)
  CDC1: đọc CKPT#LATEST
  reader.read() ──▶ từng lô
      ├─ writer.write()        gom vào bộ đệm gzip, cuộn file ở max_records_per_file / max_file_mb:
      │                        landing/<dataset>/ingest_date=<hôm nay>/part-<run_id>-<seq>-*.ndjson.gz
      └─ mỗi 20 lô: writer.flush() rồi CKPT# PARTIAL   (backlog lớn vẫn nhích dần)
  writer.flush(); CKPT#LATEST = mốc cuối      (SAU khi mọi dữ liệu đã nằm trên S3)
  manifest landing/_manifests/<dataset>/ingest_date=…/<run_id>.json   (SAU CÙNG — trigger của silver)
  CDC5: xoá file của lần chạy TRƯỚC trong partition hôm nay   (ghi mới trước, xoá cũ sau)
  ORCH#<run_id>  run_status + manifest                (chỉ khi luồng mới kích)
  RUN#  status = SUCCESS | NO_DATA | FAILED  + số liệu
```

### Hai chế độ CDC

| | CDC1: incremental | CDC5: full snapshot |
|---|---|---|
| Kéo gì | Bản ghi mới hơn checkpoint (có thể trừ `lookback_seconds`) | Toàn bộ, mỗi lần |
| Ghi raw | Append | Ghi đè partition của ngày hôm nay |
| Cần | `watermark_column`, hoặc offset nếu là Kafka | Không |
| Dùng cho | Kafka, bảng có `updated_at` | Bảng tham chiếu, API không có mốc, nguồn bị sửa hồi tố (GLEIF) |

Chọn sai chế độ là lỗi im lặng nguy hiểm nhất: chạy CDC1 trên nguồn bị sửa hồi tố sẽ
**bỏ sót thay đổi mà không báo lỗi gì**.

### Bất biến (có test canh)

1. **Ghi dữ liệu trước, tiến mốc sau.**
2. **Chỉ PARTIAL hoặc SUCCESS mới tiến mốc.** Lần chạy FAILED giữ nguyên mốc, trừ các
   ảnh chụp PARTIAL đã ghi trước đó.
3. **Manifest ghi sau cùng.** Silver thấy manifest nghĩa là mọi file của lần chạy đã có.
4. **Lỗi khi ghi `ORCH#` không làm hỏng lần chạy.** Chỉ mất event nối sang silver, có
   thể chạy silver lại bằng tay.
5. **Khoá bí mật không bao giờ nằm trong YAML hay trong raw.** Chỉ khai `secret_name`,
   giá trị lấy từ Secrets Manager. `_request` ghi xuống S3 cũng không chứa khoá.

### Reader API

- Phân trang: `none` · `page` · `offset` · `cursor`, giới hạn bởi `max_pages`.
- Retry theo cấp số nhân (tối đa 5 lần) cho 429, 5xx và lỗi mạng. Các lỗi 4xx khác thất
  bại ngay.
- `rate_limit_per_sec` cho từng entry.
- `raw_mode: response` (mặc định): một dòng = một response nguyên văn
  `{_request, _records_path, _response, _src_*}`, giữ được envelope như tổng số bản ghi
  hay khối `included` của GLEIF.

---

## 6. Dữ liệu và trạng thái

### 6.1 S3

```
landing/                                         lifecycle: xoá sau 30 ngày
  <dataset>/ingest_date=YYYY-MM-DD/
    part-<run_id>-00001-<rand>.ndjson.gz         ≤ max_records_per_file dòng/file
  _manifests/<dataset>/ingest_date=YYYY-MM-DD/
    <run_id>.json                                {job, run_id, dataset, files[], records, bytes, raw_mode, status}
  _test/…                                        khuyến nghị cho dữ liệu test (override target.dataset)
_ops/sourcing/sourcing.zip, glue_entrypoint.py
```

`<dataset>` mặc định là tên job, hoặc `target.dataset` trong YAML (ví dụ `gleif/lei_records`).

### 6.2 `job_config`: một item cho mỗi entry, Terraform seed

| Thuộc tính | Nguồn |
|---|---|
| `job_name` (khoá) | tên file YAML |
| `enabled`, `source_type`, `cdc_mode`, `schedule_enabled`, `schedule_expression`, `owner`, `description` | tách ra từ YAML để lọc được |
| `config` | **cả entry dưới dạng một chuỗi JSON**, là thứ Glue và Lambda thực sự đọc |
| `config_sha` | hash của config, để biết cấu hình đã đổi chưa |
| `last_dispatched_at`, `last_dispatch_run_id` | **[cũ]** dispatcher ghi vào (xem mục 14) |

Đừng sửa item bằng tay: lần apply sau sẽ ghi đè, và thay đổi đó không nằm trong git. Muốn
đổi tham số để test thì dùng `overrides`.

### 6.3 `job_log`: mọi thứ động, phân biệt bằng tiền tố `run_key`

| `run_key` | Ai ghi | Nội dung | TTL |
|---|---|---|---|
| `RUN#<iso>#<run_id>` | Glue | Vòng đời một lần chạy: status, số bản ghi/file/byte, lỗi, ảnh chụp cấu hình, mốc | 90 ngày |
| `CKPT#LATEST` | Glue | **Checkpoint hiện hành**: `watermarks` theo partition | **không có TTL** |
| `CKPT#<iso>#<run_id>` | Glue | Lịch sử tiến độ (PARTIAL/SUCCESS) | 90 ngày |
| `ORCH#<run_id>` | Lambda → Glue → SFN | Góc nhìn điều phối: trigger, overrides, execution, `run_status`, `manifest`, `orch_status`, lỗi | 90 ngày |

Một item `ORCH#` được ba bên lần lượt ghi vào:

```
Lambda   orch_status=STARTED, job_id, trigger, overrides, dry_run, chain, execution_arn
Glue     run_status=SUCCESS|NO_DATA|DRY_RUN, manifest
SFN      orch_status=SUCCEEDED|FAILED, glue_run_id, ended_at, err
```

Bảng silver (`fingraph-dev-slv-job-log`) dùng cùng thiết kế, thêm `TRIG#<sha(manifest)>`
để chống xử lý một manifest hai lần.

---

## 7. Lập lịch

Lịch khai trong YAML và **luôn tính theo giờ UTC**:

```yaml
schedule:
  enabled: false                        # true = có lịch
  expression: "cron(0 19 * * ? *)"      # 02:00 giờ VN mỗi ngày
```

| Trạng thái | Cách bật một job |
|---|---|
| Hiện tại (dispatcher) | `schedule.enabled: true` → apply. Dispatcher chỉ hiểu `rate(...)` và một tập con cron (giờ:phút cố định, ngày trong tháng, thứ trong tuần) |
| Sau khi chuyển (`use_orchestrator_schedules = true`) | Như trên. Terraform tạo `fingraph-dev-jobs/src-<job>`, hỗ trợ cron EventBridge đầy đủ |

Nên bật lịch **theo từng nhóm** (`sec/`, `gleif/`, `fdic/` trước), và chỉ sau khi đã chạy
tay thành công ít nhất một lần.

Lưu ý: các entry `ssi/` đang FAILED vì WAF của SSI chặn IP AWS (SETUP-LOG mục 6).

---

## 8. Chạy và kiểm thử

Các cách dưới đây sắp theo mức độ tốn kém, từ rẻ nhất.

| # | Cách | Cần AWS | Ghi gì |
|---|---|---|---|
| 1 | `pytest tests/sourcing tests/orchestrator` | không | không |
| 2 | `.\scripts\run-source.ps1 -Job <job> -Mode dry` | không | không. Gọi nguồn thật, in số bản ghi và cột |
| 3 | Lambda `{"job_id": "...", "action": "validate"}` | có | không. Trả config sau override và tham số Glue |
| 4 | Lambda `{"job_id": "...", "dry_run": true, "wait_seconds": 600}` | có | chỉ `RUN#`/`ORCH#`, không ghi S3, không tiến mốc |
| 5 | Lambda với `overrides` ghi sang `target.dataset: "_test/..."` | có | `landing/_test/...`, không chain |
| 6 | Lambda `{"job_id": "...", "chain": false, "wait_seconds": 600}` | có | dữ liệu thật, không kích silver |
| 7 | Lambda `{"job_id": "...", "wait_seconds": 840}` | có | dữ liệu thật, silver tự chạy theo |

**Gọi Lambda:**

```powershell
. .\docs\iam\assume.ps1          # credential, can MFA
aws lambda invoke --function-name fingraph-dev-trigger `
  --cli-binary-format raw-in-base64-out --cli-read-timeout 900 `
  --payload file://orchestrator/events/02_src_dry_run_wait.json out.json
Get-Content out.json
```

Cũng có thể mở Lambda console → tab **Test**, rồi dán nội dung event. Đủ 12 event mẫu và
bảng các trường nằm ở [orchestrator/README.md](../orchestrator/README.md).

Kết quả khi có `wait_seconds` trông như sau (đã rút gọn):

```json
{
  "ok": true, "job_id": "src.binance_ticker_24hr", "run_id": "3f9a…", "status": "SUCCEEDED",
  "console": "https://ap-southeast-1.console.aws.amazon.com/states/…",
  "run":  {"status": "SUCCESS", "records_written": 1, "files_written": 1, "duration_seconds": 4.2},
  "orch": {"orch_status": "SUCCEEDED", "run_status": "SUCCESS",
           "manifest": "landing/_manifests/binance/ticker_24hr/ingest_date=…/3f9a….json"}
}
```

**Các hàng rào khi test:**
- Không override được `name`, `source_type`, `enabled`, `runtime.*`, `schedule.*`.
- Override sai kiểu hoặc sai giá trị bị `from_dict()` chặn trước khi tốn tiền Glue.
- Có override thì chain tự tắt, để dữ liệu test không chảy xuống silver.
- Override trên job CDC1 mà không `dry_run` bị từ chối, trừ khi kèm
  `allow_checkpoint_advance: true`.

---

## 9. Vận hành và xử lý sự cố

### Một job có vấn đề: xem ở đâu

```
1. Lambda status      {"job_id": "src.<job>", "action": "status"}          checkpoint + 5 RUN# gần nhất
                      {"job_id": "src.<job>", "action": "status", "run_id": "<id>"}
2. ORCH#<run_id>      orch_status / run_status / err                      lỗi tầng điều phối
3. SFN console        fingraph-dev-job-runner → execution src-<job>-<run_id>
4. RUN#…              err (traceback 2000 ký tự cuối)                     lỗi tầng dữ liệu
5. CloudWatch Logs    /aws-glue/python-jobs/output | /error, stream = Glue JobRunId
                      /aws/lambda/fingraph-dev-trigger
6. Email SNS          fingraph-dev-alerts (Glue FAILED/TIMEOUT)
```

### Bảng tra nhanh

| Triệu chứng | Nguyên nhân thường gặp | Xử lý |
|---|---|---|
| `status: SKIPPED` | Entry `enabled: false` | Bật trong YAML rồi apply (entry tắt không có Glue job) |
| `"khong thay … trong …job-config"` | Entry mới chưa apply | Apply |
| `status: DUPLICATE` | Cùng tick lịch, hoặc manifest đã được xử lý | Đúng như thiết kế. Với silver, thêm `force: true` để chạy lại |
| `orch_status: STARTED` mãi không đổi | SFN đang đợi Glue (`ConcurrentRunsExceeded`) hoặc không khởi động được | Xem execution trên SFN console |
| `RUN#` FAILED, `HTTP 403` | WAF chặn IP AWS (SSI) | Chưa có cách xử lý trên Glue |
| `RUN#` FAILED, `HTTP 429` sau 5 lần thử | Rate limit | Giảm `rate_limit_per_sec` |
| `ModuleNotFoundError: sourcing` | Glue không nạp được zip | `glue_entrypoint.py` phải tự chèn zip vào `sys.path` (SETUP-LOG mục 6) |
| Silver không chạy theo | `chain=false`, NO_DATA, có override, hoặc không entry silver nào đọc dataset đó | Xem `ORCH#.manifest` và `chain`. Kích tay: `{"job_id": "slv.<job>", "manifest": "…"}` |

### Thao tác tay

```python
from sourcing.state import JobLogStore
log = JobLogStore("fingraph-dev-src-job-log")
log.read_checkpoint("<job>")          # moc hien hanh
log.recent_runs("<job>", limit=5)
log.reset_checkpoint("<job>")         # CDC1: bat keo lai tu dau (initial_watermark)
```

---

## 10. Thêm một nguồn mới

1. Tạo `sourcing_etl/sources/<nguồn>/<job>.yml`. **Tên file phải là duy nhất trên toàn
   cây** và phải trùng khoá `name`. Xem khung entry ở
   [sourcing_etl/README.md](../sourcing_etl/README.md).
2. Để `schedule.enabled: false`.
3. `pytest tests/sourcing/test_config.py` để nạp và kiểm tra mọi entry.
4. `.\scripts\run-source.ps1 -Job <job> -Mode dry` để gọi nguồn thật và xem cột.
5. Mở PR, đọc plan (sẽ có thêm một Glue job và một item config), merge, rồi Apply dev.
6. Gọi Lambda theo thứ tự `validate` → `dry_run` → `chain: false` → kiểm tra dữ liệu
   trong `landing/`.
7. Nếu cần silver: thêm entry trong `silver_etl/entries/` với `source.dataset` bằng
   dataset của job.
8. Bật `schedule.enabled: true` rồi apply.

Nguồn cần khoá API: tạo secret `fingraph/<tên>` trong Secrets Manager, rồi khai
`api.secret_name` và `api.secret_inject` trong YAML.

---

## 11. Phân quyền

| Role | Được làm | Không được làm |
|---|---|---|
| `fingraph-dev-sourcing` (Glue) | Ghi `landing/*`, `_ops/*`. Đọc/ghi `job_log`. **Chỉ đọc** `job_config`. Đọc **đúng những secret mà entry đang bật khai** trong `api.secret_name` / `jdbc.secret_name` (Terraform tự tính từ YAML; tên phải bắt đầu bằng `fingraph/`, nếu không plan bị chặn) | Sửa cấu hình của chính nó, đọc secret mà không entry nào khai, đọc secret ngoài `fingraph/` (ví dụ secret master `rds!db-…` mà RDS tự sinh) |
| `fingraph-dev-trigger` (Lambda) | Đọc bảng config (GetItem, Scan). Ghi `ORCH#`/`TRIG#`. `StartExecution`/`DescribeExecution` trên đúng `job-runner` | Chạm vào Glue, S3 |
| `fingraph-dev-job-runner` (SFN) | Chạy Glue job có tiền tố `fingraph-dev-src-*` hoặc `-slv-*`. `UpdateItem` bảng log. `PutEvents` lên default bus | Chạy Glue job ngoài hai luồng này |
| `fingraph-dev-jobs-scheduler` | Invoke Lambda trigger | Mọi thứ khác |
| Người vận hành | Assume `TerraformAdmin` (MFA) để invoke Lambda | Group `fingraph-developers` **chưa có** `lambda:InvokeFunction` |

---

## 12. Chi phí

| Thành phần | Ước tính (dev) |
|---|---|
| Glue Python Shell | ~0,0005–0,001 USD mỗi lần chạy (0,0625 DPU, tính tối thiểu 1 phút). 51 job chạy hằng ngày ≈ 1–2 USD/tháng |
| Lambda, Scheduler, EventBridge | trong free tier always-free |
| Step Functions Standard | ~5 lần chuyển trạng thái mỗi run. Vượt 4.000 lần miễn phí thì khoảng 0,025 USD / 1.000 |
| DynamoDB on-demand | vài cent |
| S3 `landing/` | nhỏ, tự xoá sau 30 ngày |

Job chạy dày (`rate(5 minutes)`) thì riêng một job đã khoảng 4–8 USD/tháng trên Glue. Loại
job này nên chuyển sang Lambda.

---

## 13. Quyết định thiết kế

| Quyết định | Vì sao | Cái giá |
|---|---|---|
| YAML trong git là nguồn chân lý, DynamoDB là bản đọc | Có review và lịch sử. Lambda và Glue đọc được mà không cần Terraform | Đổi cấu hình phải apply |
| Mỗi entry một Glue job | Timeout và thư viện riêng. `max_concurrent_runs = 1` làm khoá | 51 resource, mỗi YAML đổi là một resource đổi |
| Glue Python Shell, không dùng Spark | 0,0625 DPU là đủ cho REST API | Python 3.9, khởi động 30–60 giây |
| NDJSON gzip, response nguyên văn | Không cần pyarrow, nguồn thêm cột không làm job chết | Silver phải bóc |
| Checkpoint `CKPT#LATEST` không TTL, trong cùng bảng log | Đọc checkpoint luôn chỉ là một GetItem | Phải nhớ không gắn TTL cho nó |
| Một Lambda trigger + một SFN dùng chung | Test bằng một event, thêm luồng không phải sửa điều phối | Lambda phải biết từng loại job (`job_types.py`) |
| Lambda đọc config và truyền snapshot cho Glue | Lần chạy dùng đúng cấu hình đã kiểm tra, có chỗ áp override | Snapshot giới hạn 4.000 ký tự (hiện lớn nhất khoảng 1 KB) |
| At-least-once | Rẻ và dễ kiểm chứng hơn giao dịch hai pha giữa S3 và DynamoDB | Silver phải khử trùng |

---

## 14. Hạn chế và việc tiếp theo

| # | Việc | Mức |
|---|---|---|
| 1 | **Apply module orchestrator lên dev**, chạy các event 01 → 05 | tiếp theo |
| 2 | Bật lịch theo nhóm với `use_orchestrator_schedules = true` | sau 1 |
| 3 | **Gỡ chuỗi cũ**: dispatcher, 2 SQS + DLQ, Pipes, SFN `src-runner`, cùng rule S3 + SQS + Pipes + SFN của silver | sau 2 |
| 4 | Bug của chuỗi cũ: SFN `src-runner` đặt `expires_at = 0` cho `ORCH#`, nên TTL xoá ngay bản ghi đó | tự hết khi gỡ ở bước 3 |
| 5 | Bug của chuỗi cũ: `last_dispatched_at` nằm trong item Terraform quản lý, nên mỗi lần apply sẽ xoá nó và dispatcher kích mọi job có lịch | chỉ xảy ra khi bật lịch cũ, **không bật** |
| 6 | Tách deploy code khỏi Terraform: CI build wheel theo git sha, Glue trỏ vào phiên bản đó | cải tiến |
| 7 | `${KAFKA_BOOTSTRAP}` nằm trong tham số Glue, nên mỗi lần máy spot bị dựng lại phải apply. Chuyển sang SSM Parameter Store | cải tiến |
| 8 | `sourcing.main --dry-run` chạy tay vẫn ghi `RUN#`. Nên cho runner nhận store qua constructor để dry-run không chạm AWS | cải tiến |
| 9 | Gộp các job API thành một Glue job (hoặc Lambda) dùng chung. Khi đó cần lease trong DynamoDB thay cho `max_concurrent_runs` | cân nhắc |
| 10 | Cấp `lambda:InvokeFunction` cho group `fingraph-developers`. Nối orchestrator vào `envs/prd` khi prd có silver | khi cần |
| 11 | Alarm cho SFN `ExecutionsFailed` và cho độ sâu DLQ | cải tiến |
