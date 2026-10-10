# Kiến trúc raw → silver — thiết kế, triển khai, vận hành

Tầng **silver** đọc dữ liệu thô mà sourcing ghi ở `landing/` (NDJSON gzip, giữ nguyên
response), ép về schema khai trong entry, gắn cột kỹ thuật `ds_*`, khử trùng lặp, kiểm tra
chất lượng, rồi ghi vào bảng **Iceberg** `track_a_silver.*` bằng một trong ba writer:
SCD2, SCD4a snapshot hoặc simple.

Tài liệu này mô tả toàn cảnh: thành phần, luồng chạy, bên trong job, dữ liệu và trạng
thái, cách test, cách vận hành, các tình huống có thể xảy ra, điểm mạnh, điểm yếu và việc
cần sửa.

| Muốn biết | Đọc |
|---|---|
| Mọi khoá của một entry silver | [silver_etl/README.md](../silver_etl/README.md) |
| Tầng phía trước (sourcing, orchestrator) | [SOURCING-ARCHITECTURE.md](SOURCING-ARCHITECTURE.md) |
| Lambda trigger, event mẫu | [orchestrator/README.md](../orchestrator/README.md) |
| Lệnh đã chạy, lỗi đã gặp | `docs/SETUP-LOG.md` của **repo hạ tầng** (`lakehouse_ontology_project`) |

> **Trạng thái (09-29).** Framework, 5 entry mẫu, module Terraform `silver` và phần silver
> của orchestrator đã viết xong và có test, gồm 14 test writer chạy trên **Spark 3.5.4 +
> Iceberg 1.7.1 thật** (cùng phiên bản Glue 5.0). Đã chạy đầu-cuối trên máy với dữ liệu
> thật của GLEIF và FDIC. **Chưa `terraform apply`**, nên trên AWS chưa có bảng silver nào.

---

## Mục lục

1. [Tổng quan](#1-tổng-quan)
2. [Thành phần](#2-thành-phần)
3. [Từ code đến AWS](#3-từ-code-đến-aws)
4. [Một lần chạy đi qua những đâu](#4-một-lần-chạy-đi-qua-những-đâu)
5. [Bên trong Glue job silver](#5-bên-trong-glue-job-silver)
6. [Writer](#6-writer)
7. [DDL và tiến hoá schema](#7-ddl-và-tiến-hoá-schema)
8. [Dữ liệu và trạng thái](#8-dữ-liệu-và-trạng-thái)
9. [Chạy và kiểm thử](#9-chạy-và-kiểm-thử)
10. [Vận hành và xử lý sự cố](#10-vận-hành-và-xử-lý-sự-cố)
11. [Thêm một bảng silver](#11-thêm-một-bảng-silver)
12. [Các tình huống có thể xảy ra](#12-các-tình-huống-có-thể-xảy-ra)
13. [Điểm mạnh](#13-điểm-mạnh)
14. [Điểm yếu và rủi ro](#14-điểm-yếu-và-rủi-ro)
15. [Việc cần sửa](#15-việc-cần-sửa)
16. [Quyết định thiết kế](#16-quyết-định-thiết-kế)

---

## 1. Tổng quan

```
 RAW (sourcing)                         ĐIỀU PHỐI                           CHẠY                        ĐÍCH
 ──────────────                         ─────────                           ────                        ────
 landing/<dataset>/ingest_date=D/                                                                  track_a_silver.<bảng>
   part-<run>-*.ndjson.gz      ┌──▶ SFN job-runner bắn "Job Run Succeeded" ─┐                      (Iceberg, Glue Catalog)
 landing/_manifests/<dataset>/ │                                            ▼                             ▲
   D/<run_id>.json  ───────────┤                                 Lambda trigger ──▶ SFN job-runner ──▶ Glue slv-<entry>
   (ghi SAU CÙNG)              │                                  (tìm entry đọc     (.sync, retry)   (Spark 5.0)
                               └──▶ [phụ] S3 event ─▶ SQS ─▶ Lambda router ─▶ FIFO ─▶ Pipes ─▶ SFN slv-runner ─┘
```

Năm nguyên tắc:

1. **Thêm một bảng silver là thêm một file YAML.** Terraform sinh Glue job và seed
   `slv-job-config`; job đọc cùng cấu hình đó lúc chạy. Chỉ khi cần một **kiểu ghi** mới
   mới phải sửa Python.
2. **Schema do silver quyết, không phải raw.** Raw giữ nguyên văn response (cả envelope).
   Entry khai cột nào, lấy từ đường nào, ép về kiểu gì.
3. **Đọc đúng lần chạy, không đoán.** Silver đọc danh sách file trong **manifest** của một
   lần chạy sourcing, không liệt kê thư mục.
4. **Mọi writer chạy lại được.** Chạy lại cùng manifest hoặc cùng ngày cho cùng trạng thái
   bảng. Retry của Step Functions, giao lại của EventBridge hay người bấm chạy lại đều không
   nhân đôi dữ liệu.
5. **Một giá trị hỏng không giết cả lô.** Ép kiểu bằng `try_cast`, đếm lỗi từng cột; quality
   quyết định có ghi hay không.

**Về tầng bronze.** README gốc vẽ `bronze → silver`. Framework này đi thẳng từ `landing/`
lên silver: `landing/` đã đóng vai bronze (bất biến, nguyên văn, có partition ngày, giữ
30 ngày theo lifecycle). Thêm một bản Iceberg "raw có kiểu" ở giữa sẽ nhân đôi dung lượng
mà không thêm thông tin. Riêng luồng Kafka vẫn có `jobs/glue/bronze_ingest.py`.

---

## 2. Thành phần

### Trong repo

```
silver/                    FRAMEWORK — chạy trong Glue job (Spark)
  config.py                  schema entry, kiểm tra lúc nạp — KHÔNG import pyspark
  types.py                   dựng biểu thức ép kiểu (chuỗi SQL), schema from_json từ path
  extract.py                 đọc dòng text -> cột envelope -> nổ mảng -> bóc cột
  transform.py               ép kiểu, cột ds_*, khử trùng lặp, quality
  writers/                   scd2 · scd4a · simple
  ddl.py                     sinh DDL, ensure_table, tách câu lệnh, CLI generate
  runner.py                  một lần chạy: giải đầu vào -> extract -> transform -> writer -> log
  state.py                   SilverLogStore (kế thừa sourcing.state) + TRIG#
  main.py                    điểm vào: --MODE run | ddl | apply_ddl
  lambda_router.py           [luồng phụ] Lambda nghe S3 manifest

silver_etl/
  entries/<nguồn>/<bảng>.yml CẤU HÌNH — mỗi file một bảng, tên file = tên job
  ddl/<database>/<bảng>.sql  DDL viết tay (tuỳ chọn)
  glue_entrypoint.py         script Glue, tự thêm silver.zip vào sys.path

orchestrator/job_types.py  SilverType: tên Glue job, tham số, TRIG#, consumers_of()
terraform/modules/silver/  Glue job mỗi entry, slv-ddl-apply, 2 bảng DynamoDB, chuỗi S3 event
tests/silver/              config · types · ddl · router · runner · 14 test Spark thật
```

### Trên AWS (dev, sau khi apply)

| Resource | Tên | Vai trò |
|---|---|---|
| Glue job × 5 | `fingraph-dev-slv-<entry>` | Spark 5.0, G.1X × 2 mặc định, `max_concurrent_runs = 1` |
| Glue job | `fingraph-dev-slv-ddl-apply` | Chạy mọi file `silver_etl/ddl/**/*.sql` |
| S3 | `_ops/silver/silver.zip`, `glue_entrypoint.py`, `ddl/**` | Code và DDL |
| DynamoDB | `fingraph-dev-slv-job-config` | Seed từ YAML; cột `source_dataset` để tìm entry theo dataset |
| DynamoDB | `fingraph-dev-slv-job-log` | `RUN#`, `TRIG#`, `ORCH#` (TTL 90 ngày) |
| Lambda + SFN | `fingraph-dev-trigger`, `fingraph-dev-job-runner` | **Luồng chính**, dùng chung với sourcing (module `orchestrator`) |
| EventBridge rule | `fingraph-dev-job-succeeded` | Sourcing xong thì gọi Lambda trigger để kích silver |
| S3 notification | bucket lakehouse, `eventbridge = true` | Bật một lần cho mọi module |
| EventBridge rule, 2 SQS (+DLQ), Lambda, Pipes, SFN | `fingraph-dev-slv-*` | **Luồng phụ** qua S3 event, sẽ gỡ (mục 15) |

---

## 3. Từ code đến AWS

```
 silver/*.py + sourcing/*.py  ──zip──▶ S3 _ops/silver/silver.zip       (--extra-py-files của Glue)
 silver_etl/glue_entrypoint.py ─────▶ S3 _ops/silver/glue_entrypoint.py (script_location)
 silver_etl/ddl/**/*.sql      ──────▶ S3 _ops/silver/ddl/**             (--DDL_PREFIX)
 silver_etl/entries/**/*.yml  ──────▶ 1 aws_glue_job mỗi entry bật + 1 item slv-job-config mỗi entry
 orchestrator + sourcing + silver ──zip──▶ Lambda fingraph-dev-trigger  (import silver.config, không pyspark)
```

Zip chứa cả `sourcing/` vì silver dùng lại `JobLogStore`. Sửa YAML hay Python đều **phải
apply** mới có hiệu lực trên AWS. Lambda chỉ import `silver.config`, `silver.state` và
`silver.lambda_router`, nên zip Lambda không kéo pyspark.

---

## 4. Một lần chạy đi qua những đâu

### 4.1 Luồng chính: sourcing xong thì silver chạy theo

```
 ① Glue src-<job> ghi landing/…/part-*.ndjson.gz
                  ghi landing/_manifests/<dataset>/ingest_date=D/<run_id>.json   (SAU CÙNG)
                  ghi manifest vào ORCH#<run_id>
        │
 ② SFN job-runner: RecordSuccess đọc lại ORCH# (ALL_NEW) → có manifest và chain=true
        │           → PutEvents "Job Run Succeeded" {job_type: src, manifest, run_id}
        ▼
 ③ Rule fingraph-dev-job-succeeded → Lambda trigger
        · SilverType.consumers_of(): tách dataset + ngày từ manifest,
          Scan slv-job-config tìm entry ĐANG BẬT có source_dataset khớp
          (một dataset nuôi nhiều bảng: gleif/lei_records → gleif_legal_entity + gleif_parent_entity)
        · mỗi entry: claim TRIG#<sha(manifest)> CÓ ĐIỀU KIỆN   (đã có → bỏ qua)
        · ORCH#<run_id> + StartExecution job-runner
        ▼
 ④ SFN job-runner: Glue slv-<entry> .sync
        --JOB_NAME --RUN_ID --MANIFEST --DATA_DATE --REBUILD_FROM   ("-" = không có)
        retry ConcurrentRunsExceeded 60s × 1,2ⁿ, tối đa 30 lần  → TUẦN TỰ THEO BẢNG
        ▼
 ⑤ Glue job (mục 5) → RUN# + metrics, TRIG# = SUCCESS | NO_DATA | FAILED
```

Silver không ghi manifest vào `ORCH#`, nên job-runner **không** bắn event tiếp. Khi có tầng
gold, chỉ cần silver ghi manifest (hoặc tên bảng + snapshot id) và thêm một `JobType`.

### 4.2 Luồng phụ: S3 event (module silver, sẽ gỡ)

```
S3 PutObject landing/_manifests/** → EventBridge rule → SQS slv-trigger (+DLQ)
  → Lambda slv-router (batch 10, ReportBatchItemFailures) → claim TRIG#
  → SQS slv-run.fifo (MessageGroupId = tên job) → Pipes → SFN slv-runner → Glue
```

Cả hai luồng cùng nhận **một** manifest. `TRIG#` có điều kiện bảo đảm chỉ luồng đến trước
chạy; luồng sau thấy đã có claim thì bỏ qua. Luồng phụ còn hữu ích cho file do **bên ngoài**
đẩy vào `landing/`, vì những file đó không đi qua job-runner. Luồng chính rõ hơn vì nơi biết
chắc job đã xong mới là nơi báo.

### 4.3 Gọi tay: chạy lại, backfill

| Việc | Event gửi Lambda `fingraph-dev-trigger` |
|---|---|
| Chạy lại một manifest đã xử lý | `{"job_id":"slv.binance_ticker_24h","manifest":"landing/_manifests/…/<run>.json","force":true}` |
| Chạy theo ngày (đọc **mọi** file của ngày) | `{"job_id":"slv.fdic_institution","data_date":"2026-09-28"}` |
| Viết lại lịch sử SCD2 từ một ngày | `{"job_id":"slv.gleif_legal_entity","data_date":"2026-09-20","rebuild_from":"2026-09-20"}` |
| Xem trước tham số, không chạy | thêm `"action":"validate"` |
| Đợi kết quả | thêm `"wait_seconds":840` (CLI cần `--cli-read-timeout 900`) |

Event mẫu: [orchestrator/events/06_slv_run_by_date.json](../orchestrator/events/06_slv_run_by_date.json),
[07_slv_rerun_manifest.json](../orchestrator/events/07_slv_rerun_manifest.json).

---

## 5. Bên trong Glue job silver

```
 main.py  spark.sql.session.timeZone = UTC  (cả trong --conf của Glue)
   │ đọc entry từ slv-job-config (hoặc --config file.yml khi chạy tay)
   ▼
 runner.run()
   RUN# RUNNING
   ① resolve_inputs   manifest → danh sách file + raw_mode
                      data_date → liệt kê *.ndjson.gz của ngày, mượn raw_mode từ manifest cùng ngày
   ② read_lines       spark.read.text(files) — KHÔNG suy schema; ingest_date lấy từ đường dẫn
   ③ envelope         cột from: response | request | line — get_json_object Ở MỨC DÒNG
   ④ explode          records_path → from_json(…, array<string>) → explode; BỎ DÒNG RAW NGAY
   ⑤ bóc cột          một lần from_json, schema sinh từ các path của entry (lá = string)
   ⑥ ép kiểu          transform → try_cast → default → expr ; đếm lỗi ép kiểu từng cột
   ⑦ filter + ds_*    ds_key, ds_hash, ds_record_status, ds_partition_date, ds_cdc_index, …
   ⑧ dedup + quality  giữ bản ds_cdc_index lớn nhất mỗi ds_key; not_null, cast error, min_rows
   ⑨ ensure_table     tạo bảng (file DDL hoặc sinh) + ALTER ADD COLUMNS
      writer.write()  scd2 | scd4a | simple
   RUN# SUCCESS | NO_DATA | FAILED  (+ metrics) ; TRIG# cùng trạng thái
```

### Ba hình dạng dòng raw

| `shape` | Nguồn | Bản ghi nằm ở |
|---|---|---|
| `api_response` | API `raw_mode: response` (mặc định) | `_response.<records_path>`; response là object thì coi là mảng một phần tử |
| `records` | JDBC, file S3, API `raw_mode: records` | cả dòng |
| `kafka_message` | Kafka | chuỗi JSON trong `value` |

Mặc định lấy theo `raw_mode` trong manifest, nên entry hầu như không phải khai `shape`.

### Bóc cột

- `path` là đường chấm, tên có dấu `-` vẫn dùng được (`relationships.direct-parent.data.id`).
- Các path được gom thành **một** schema `from_json` với lá là chuỗi, nên chỉ parse một lần.
  Path xung đột (`a.b` là lá, `a.b.c` là nhánh) được bóc riêng bằng `get_json_object`.
- Object hay mảng lồng nhau đến dưới dạng chuỗi JSON nguyên văn; khai `type: array<…>` hoặc
  `struct<…>` thì `from_json` ép lại.
- `from: response` lấy từ envelope: SEC frames `tag`/`uom`/`ccp`, GLEIF `meta.goldenCopy`.
- `keep: false` là cột trung gian, dùng để tính cột khác rồi bỏ. `expr` là cột tính sau khi ép kiểu.

### Ép kiểu

| `type` | Biểu thức | Ghi chú |
|---|---|---|
| `string` | giữ nguyên | |
| `int`, `bigint`, `double`, `decimal(p,s)` | `try_cast(trim(x) AS …)` | decimal làm tròn theo scale |
| `boolean` | `true/t/y/yes/1`, `false/f/n/no/0` | FDIC dùng 0/1 |
| `date`, `timestamp` | `try_cast` (ISO 8601) | |
| … + `format: MM/dd/yyyy` | `try_to_timestamp(x, 'MM/dd/yyyy')` | |
| … + `format: epoch_ms` / `epoch_s` | `timestamp_millis` / `timestamp_seconds` | Binance `closeTime` |
| `array<…>`, `map<…>`, `struct<…>` | `from_json(x, '<type>')` | |

**Lỗi ép kiểu** được tính khi nguồn có giá trị (khác NULL, khác rỗng) nhưng kết quả là NULL.
Tỷ lệ vượt `quality.max_cast_error_ratio` thì job FAILED và bảng không đổi.

### Cột kỹ thuật

| Cột | Kiểu | Cách tính |
|---|---|---|
| `ds_key` | string | SHA-256 của `technical.keys`; không khai key thì mọi cột nghiệp vụ là khoá |
| `ds_hash` | string | SHA-256 các cột nghiệp vụ trừ `hash: false`; NULL có dấu riêng, khác chuỗi rỗng |
| `ds_record_status` | string | `I` / `U` / `D` / `N`; `technical.delete_when` đánh dấu lệnh xoá từ nguồn CDC |
| `ds_partition_date` | date | `ingest_date` (mặc định) · `run_date` · một cột · biểu thức |
| `ds_cdc_index` | bigint | timestamp thành micro-giây, số giữ nguyên; mặc định `_src_ingest_ts` |
| `ds_etl_timestamp` | timestamp | lúc ghi |
| `ds_etl_job_id` | string | run_id |
| `ds_source` | string | manifest (hoặc prefix ngày) sinh ra dòng; khoá idempotent của `append` |
| `ds_valid_from`, `ds_valid_to`, `ds_is_current` | date, date, boolean | chỉ SCD2; `9999-12-31` là hiện hành |

### Khử trùng lặp và quality

Mỗi `ds_key` giữ dòng có `ds_cdc_index` lớn nhất; hoà thì lấy dòng raw kéo sau. Số dòng trùng
được đếm **trước** khi khử và báo theo `quality.duplicate_keys` (`warn` hoặc `fail`).

| Kiểm tra | Khoá | Mặc định |
|---|---|---|
| Cột bắt buộc có giá trị | `not_null` | không kiểm |
| Tỷ lệ ép kiểu hỏng | `max_cast_error_ratio` | `0`: một giá trị hỏng cũng fail |
| Trùng key trong lô | `duplicate_keys` | `warn` |
| Số dòng tối thiểu | `min_rows` | `0` |
| Hỏng thì | `on_fail` | `fail` |

---

## 6. Writer

### 6.1 Chọn writer

| Dữ liệu | Writer | Entry mẫu |
|---|---|---|
| Thực thể đổi chậm, cần lịch sử từng lần đổi | `scd2` | `gleif_legal_entity` |
| Ảnh chụp đầy đủ mỗi ngày, cần biết hôm nay khác hôm qua gì | `scd4a` | `fdic_institution` |
| Sự kiện, chuỗi thời gian | `simple` + `append` | `binance_ticker_24h`, `sec_frame_revenues` |
| Ảnh chụp theo ngày, không cần I/U/D | `simple` + `overwrite_partition` | `gleif_parent_entity` |
| Bảng tham chiếu nhỏ | `simple` + `overwrite_table` | (mã quốc gia, loại hình pháp lý) |

### 6.2 SCD2

So lô (đã khử trùng) với phiên bản **hiện hành** của từng key:

| Trường hợp | Hành động | Status bản mới |
|---|---|---|
| Key mới | chèn | `I` |
| Key đang ở trạng thái `D` rồi xuất hiện lại | đóng bản D, chèn | `I` |
| `ds_hash` khác | đóng bản cũ, chèn | `U` |
| Giống hệt | bỏ qua, không sinh phiên bản rác | |
| Nguồn báo xoá (`delete_when`) | đóng bản cũ, chèn "bia mộ" | `D` |
| Vắng mặt, chỉ khi `writer.snapshot: true` | đóng bản cũ, chèn bia mộ mang giá trị cũ | `D` |

Tất cả gói trong **một** câu `MERGE`, tức một commit Iceberg. Mỗi thay đổi cần hai hành động
(đóng bản cũ và chèn bản mới) mà một dòng nguồn chỉ khớp được một nhánh, nên dòng được nhân
đôi: một bản có `_merge_key = ds_key` để khớp và đóng, một bản `_merge_key = NULL` để chèn.

**Chạy lại và thứ tự:**

- Lô có ngày nhỏ hơn hoặc bằng ngày lớn nhất trong bảng, và không có ngày nào **lớn hơn** lô:
  hoàn tác các phiên bản từ ngày đó (DELETE rồi mở lại bản bị đóng) rồi áp lại. Chạy lại ngày D
  cho cùng kết quả.
- Bảng đã có phiên bản **mới hơn** lô (dữ liệu đến trễ): **từ chối** với `OutOfOrderError`.
  Muốn viết lại lịch sử thì chạy có chủ đích với `rebuild_from` rồi chạy tiếp các ngày sau theo thứ tự.
- `changes` được chốt bằng `localCheckpoint(eager=True)` **trước** `MERGE`. Nếu để lazy, Spark
  tính lại sau khi bảng đã đổi và số liệu sai (bug test đã bắt, xem mục 9).

### 6.3 SCD4a snapshot

Một bảng chứa cả hiện hành và lịch sử, partition `ds_partition_date`, mỗi partition là **ảnh
chụp đầy đủ** của một ngày. Chạy ngày D chỉ ghi đè partition D.

- "main" = partition mới nhất; "hist" = các partition cũ hơn.
- Status so với ảnh **liền trước** (ngày lớn nhất nhỏ hơn D): `I` mới, `U` đổi, `N` không đổi,
  `D` biến mất (dòng D mang giá trị của ảnh trước, `emit_deletes`).
- Một lô chỉ được một ngày.

Truy vấn hiện hành:

```sql
SELECT * FROM track_a_silver.fdic_institution
WHERE ds_partition_date = (SELECT max(ds_partition_date) FROM track_a_silver.fdic_institution)
  AND ds_record_status <> 'D';
```

### 6.4 Simple

| Mode | Làm gì | Idempotent nhờ |
|---|---|---|
| `append` | `DELETE WHERE ds_source = <manifest>` rồi thêm | `ds_source` |
| `overwrite_partition` | Iceberg dynamic overwrite các ngày có trong lô | partition |
| `overwrite_table` | thay cả bảng; **từ chối lô rỗng** | cả bảng |

Nhiều entry có thể cùng `append` vào một bảng (ví dụ các frame SEC khác nhau vào
`sec_financial_frame`), vì mỗi entry có `ds_source` riêng.

---

## 7. DDL và tiến hoá schema

Có hai cách để có bảng:

| Cách | Khi nào |
|---|---|
| `target.ddl: track_a_silver/x.sql` | Cần kiểm soát tay: partition transform (`bucket`, `days`), sort order, TBLPROPERTIES, comment |
| Không khai | Sinh từ `columns` + cột kỹ thuật. Đủ cho đa số bảng |

```powershell
python -m silver.ddl generate                                   # in DDL mọi entry
python -m silver.ddl generate --out silver_etl/ddl --only <entry> # ghi file để sửa tay
aws glue start-job-run --job-name fingraph-dev-slv-ddl-apply      # tạo mọi bảng trong ddl/
```

Mỗi lần chạy, `ensure_table` kiểm tra bảng thật:

| Bảng | Hành động |
|---|---|
| Chưa có | `CREATE NAMESPACE IF NOT EXISTS` + chạy DDL (file hoặc sinh) |
| Thiếu cột entry đang khai | `ALTER TABLE ADD COLUMNS` (`schema_evolution: add_columns`, mặc định); `strict` thì lỗi |
| Cột lệch kiểu | **Lỗi.** Đổi kiểu là thay đổi có chủ đích, làm tay |

Thuộc tính mặc định: `format-version 2`, nén `zstd`, **merge-on-read** cho delete/update/merge.
Ghi nhỏ nhanh hơn, đổi lại cần compaction định kỳ (mục 15).

Bộ tách câu lệnh quét từng ký tự: `;` và `--` nằm trong chuỗi hoặc tên trích dẫn không phải ranh giới.

---

## 8. Dữ liệu và trạng thái

### 8.1 S3

```
landing/<dataset>/ingest_date=D/part-<run_id>-NNNNN-<rand>.ndjson.gz   raw — sourcing ghi
landing/_manifests/<dataset>/ingest_date=D/<run_id>.json               manifest — trigger của silver
track-a/track_a_silver.db/<bảng>/data|metadata                         Iceberg (GlueCatalog)
_ops/silver/{silver.zip, glue_entrypoint.py, ddl/**}                   code
_ops/spark-logs/silver/                                                Spark UI history
```

Manifest mẫu:

```json
{"job":"fdic_institutions","run_id":"37b8…","dataset":"fdic/institutions",
 "partition":"ingest_date=2026-09-28","cdc_mode":"cdc5","status":"SUCCESS","raw_mode":"response",
 "bucket":"fingraph-lakehouse-dev-…","files":["landing/fdic/institutions/ingest_date=2026-09-28/part-…"],
 "records":28,"bytes":8104521,"created_at":"2026-09-28T11:31:20+00:00"}
```

### 8.2 `slv-job-config`

Mỗi entry một item: `job_name`, `enabled`, **`source_dataset`** (Lambda lọc theo cột này),
`writer`, `target_table`, `config` (JSON đầy đủ), `config_sha`. Terraform ghi đè mỗi lần
apply; đừng sửa tay.

### 8.3 `slv-job-log`

| `run_key` | Ai ghi | Nội dung |
|---|---|---|
| `RUN#<iso>#<run_id>` | Glue job | `status`, `metrics` (rows_in/out, inserted/updated/deleted/unchanged, duplicate_keys, cast_errors, ddl created/added, seconds), `err` |
| `TRIG#<sha256(manifest)[:32]>` | Lambda claim, Glue cập nhật | `manifest`, `status` QUEUED → SUCCESS / NO_DATA / FAILED |
| `ORCH#<run_id>` | Lambda trigger, SFN | `orch_status`, `glue_run_id`, lỗi hạ tầng |

`TRIG#` ở trạng thái `FAILED` cho phép giành lại (chạy lại sau khi sửa). `force: true` bỏ qua claim.

---

## 9. Chạy và kiểm thử

```powershell
pytest tests/silver                  # 52 test; 14 test Spark cần pyspark==3.5.4 + Java (+ winutils trên Windows)
python -m silver.ddl generate        # không cần Spark
python -m silver.main --config silver_etl/entries/fdic/fdic_institution.yml `
       --MANIFEST <file manifest cục bộ> --RAW_BUCKET ""      # chạy tay, cần catalog glue_catalog cấu hình sẵn
```

Test Spark dùng Spark 3.5.4 + Iceberg 1.7.1, catalog Hadoop trên đĩa nhưng **đặt tên
`glue_catalog`**, nên SQL của writer y hệt khi chạy trên Glue. Fixture `RawLake` ghi raw
đúng định dạng sourcing (NDJSON.gz + manifest).

| Nhóm test | Kiểm |
|---|---|
| `test_config_types_ddl.py` | 11 kiểu entry sai bị chặn lúc nạp, biểu thức ép kiểu, path có `-`, DDL, bộ tách câu lệnh |
| `test_router.py` | một dataset nuôi nhiều bảng, manifest đến hai lần, TRIG# FAILED giành lại, message hỏng không kéo cả lô |
| `test_runner_inputs.py` | manifest sai dataset, backfill theo ngày mượn raw_mode |
| `test_spark_writers.py` | vòng đời SCD2, không đổi thì không sinh phiên bản, chạy lại cùng ngày, đến trễ bị chặn, tái xuất hiện, SCD4a I/U/N/D, append chạy lại, overwrite_partition, lô rỗng, lỗi ép kiểu, UTC, thêm cột |

**Chạy đầu-cuối với dữ liệu thật (09-28, trên máy):**

| Entry | Kết quả |
|---|---|
| `gleif_legal_entity` (scd2) | 374 pháp nhân VN, 0 lỗi ép kiểu, bảng tạo từ file DDL |
| `gleif_parent_entity` (simple) | 132 bản ghi `included`, khử trùng còn 124 công ty mẹ |
| `fdic_institution` (scd4a) | 27.834 ngân hàng, 0 lỗi ép kiểu, 39 giây |
| Cạnh sở hữu | 110/110 khớp nút mẹ: Vinhomes → Vingroup, Thép Hoà Phát Dung Quất → Hoà Phát, Chứng khoán BIDV → BIDV |

**Bug test bắt được trước khi lên AWS:**

| Bug | Hậu quả nếu lọt |
|---|---|
| Tách DDL theo `;` cắt đôi COMMENT chứa `;` | Không tạo được bảng SCD2 nào |
| SCD2 tính số liệu sau `MERGE` (lazy) | `inserted=0` trong job_log dù đã chèn |
| Bản ghi sau explode vẫn mang cả dòng raw (3 MB × 27.834) | FDIC hết bộ nhớ |
| Thứ tự path theo hash của `set` | Schema `from_json` khác nhau giữa các lần chạy |
| Spark lấy múi giờ máy | `ds_partition_date` lệch khi chạy tay trên máy UTC+7 |

---

## 10. Vận hành và xử lý sự cố

### Một bảng có vấn đề: xem theo thứ tự

1. `slv-job-log`, `RUN#` mới nhất của entry: `status`, `metrics`, `err`.
2. `TRIG#` của manifest: `QUEUED` quá lâu nghĩa là Glue chưa từng chạy (xem SFN).
3. SFN `fingraph-dev-job-runner`, execution `slv-<entry>-<run_id>`: retry ConcurrentRuns, lỗi Glue.
4. CloudWatch `/aws-glue/jobs/error`, stream = JobRunId; Spark UI trong `_ops/spark-logs/silver/`.
5. Iceberg: `SELECT * FROM glue_catalog.track_a_silver.<bảng>.snapshots` để xem commit nào đã vào.

### Bảng tra nhanh

| Triệu chứng | Nghĩa là | Làm gì |
|---|---|---|
| `QualityError … khong ep duoc kieu` | Nguồn đổi định dạng hoặc entry khai sai kiểu | Xem `metrics.cast_errors`; sửa `type`/`format`, hoặc nới `max_cast_error_ratio` |
| `QualityError … NULL o cot not_null` | Nguồn thiếu trường, hoặc sai `path` | Kiểm path trên một dòng raw thật |
| `OutOfOrderError` | Lô SCD2 cũ hơn phiên bản đã có | Chạy có chủ đích với `rebuild_from`, rồi các ngày sau theo thứ tự |
| `lech kieu cot` | Entry đổi `type` của cột đã có | `ALTER TABLE … ALTER COLUMN` tay (chỉ nới kiểu an toàn) hoặc tạo cột mới |
| `manifest thuoc dataset …` | Gọi tay sai entry cho manifest | Sửa `job_id` |
| `scd4a nhan mot ngay moi lo` | Backfill theo `partition_date: column` trải nhiều ngày | Tách lô theo ngày |
| RUN# `NO_DATA` | Manifest rỗng, response rỗng, hoặc filter loại hết | Bình thường nếu nguồn không có dữ liệu |
| Duplicate lớn trong `warnings` | Nguồn trả trùng, hoặc `keys` khai thiếu | Xem lại `technical.keys` |
| SFN retry ConcurrentRuns rất lâu | Lần chạy trước cùng bảng chưa xong hoặc treo | Xem Glue run đang RUNNING |

### Thao tác tay

| Việc | Cách |
|---|---|
| Tạo lại mọi bảng từ DDL | `aws glue start-job-run --job-name fingraph-dev-slv-ddl-apply` |
| Xoá một bảng để nạp lại từ đầu | `DROP TABLE glue_catalog.track_a_silver.<bảng> PURGE` (Athena/Spark), rồi chạy theo ngày |
| Quay bảng về snapshot trước | `CALL glue_catalog.system.rollback_to_snapshot('track_a_silver.<bảng>', <id>)` |
| Chạy lại manifest | event có `"force": true` |

---

## 11. Thêm một bảng silver

```
1. Kéo một dòng raw thật để xem cấu trúc:  .\scripts\run-source.ps1 -Job <nguồn> -Mode dry
2. Tạo silver_etl/entries/<nguồn>/<bảng>.yml   (source.dataset khớp dataset của sourcing)
3. (tuỳ chọn) python -m silver.ddl generate --out silver_etl/ddl --only <bảng>, sửa tay, khai target.ddl
4. pytest tests/silver/test_config_types_ddl.py   — test canh mọi entry thật nạp được
5. terraform plan / apply                       — 1 Glue job + 1 item job_config
6. Kích sourcing của dataset đó, hoặc gửi {"job_id":"slv.<bảng>","data_date":"…"}
```

---

## 12. Các tình huống có thể xảy ra

| # | Tình huống | Hệ thống xử lý | Kết quả |
|---|---|---|---|
| 1 | Ngày bình thường: sourcing chạy, có dữ liệu | Manifest → event → claim TRIG# → Glue | Bảng cập nhật, RUN# SUCCESS |
| 2 | Cùng manifest đến hai lần (EventBridge giao lại, hai luồng cùng nghe) | Claim TRIG# có điều kiện | Chỉ chạy một lần |
| 3 | SFN retry sau khi Glue đã ghi một phần | Writer idempotent (ds_source / partition / rollback SCD2) | Không nhân đôi |
| 4 | Hai manifest cùng bảng đến sát nhau | `max_concurrent_runs = 1` + retry của SFN | Chạy lần lượt; thứ tự **không bảo đảm** (xem 14) |
| 5 | Sourcing NO_DATA | Không có manifest | Silver không chạy |
| 6 | API trả 200 kèm `{"error": …}` | Raw giữ response; explode ra 0 bản ghi | Silver NO_DATA, bảng không đổi |
| 7 | Nguồn thêm trường mới | Raw vẫn ghi; entry chưa khai thì bỏ qua | Không lỗi; thêm cột vào entry → `ALTER ADD COLUMNS` |
| 8 | Nguồn đổi định dạng ngày / số | `try_cast` ra NULL, đếm cast error | Quality fail, bảng giữ nguyên (hoặc ghi nếu dưới ngưỡng) |
| 9 | Nguồn đổi tên trường | Cột thành NULL | Chỉ bắt được nếu cột có `not_null` — nên khai cho cột quan trọng |
| 10 | Nguồn trả thiếu trang (ví dụ FDIC 5.000 thay vì 27.834) | `min_rows` | Fail thay vì SCD4a/SCD2 đánh dấu xoá hàng loạt |
| 11 | Một key biến mất khỏi ảnh chụp | SCD2 `snapshot: true` / SCD4a `emit_deletes` | Phiên bản `D` |
| 12 | Key đã xoá xuất hiện lại | SCD2 | Phiên bản `I` mới, lịch sử giữ nguyên |
| 13 | Trường chỉ đổi hằng ngày mà không mang nghĩa (ngày xuất bản) | `hash: false` | Không sinh phiên bản rác |
| 14 | Trùng key trong một lô | dedup theo `ds_cdc_index` | Giữ bản mới nhất; cảnh báo hoặc fail |
| 15 | Dữ liệu SCD2 đến trễ | `OutOfOrderError` | Fail có chủ đích; cần `rebuild_from` |
| 16 | Chạy lại SCD2 cùng ngày | Rollback từ ngày đó rồi MERGE | Cùng kết quả |
| 17 | Backfill SCD4a một ngày cũ | Ghi đè partition ngày đó | Status của các ngày **sau** thành cũ; phải chạy lại theo thứ tự |
| 18 | Response trả object thay vì mảng (SEC submissions) | Bọc thành mảng một phần tử | Một bản ghi |
| 19 | Một dataset nuôi nhiều bảng | `consumers_of` trả mọi entry khớp | Mỗi bảng một execution, song song |
| 20 | Nhiều entry cùng ghi một bảng | `append` + `ds_source` riêng | Không đè nhau; Iceberg tự retry commit xung đột |
| 21 | Glue chết giữa DELETE và MERGE (SCD2) | Lần sau làm lại rollback + MERGE | Đúng sau khi chạy lại; người đọc có thể thấy trạng thái giữa chừng |
| 22 | Response rất lớn (Binance exchangeInfo 18 MB một dòng) | Envelope lấy ở mức dòng, dòng raw bỏ sau explode | Chạy được; một dòng vẫn là một task |
| 23 | Entry đổi kiểu cột đã có | `ensure_table` báo lệch kiểu | Fail, không ghi |
| 24 | Nguồn bị WAF chặn trên AWS (SSI) | Sourcing không có manifest | Silver không chạy; cần sourcing chạy từ máy có IP VN |
| 25 | File bên ngoài đẩy vào `landing/` | Không qua job-runner → chỉ luồng phụ S3 event bắt được | Sẽ mất đường này khi gỡ luồng phụ (xem 15) |
| 26 | TRIG# claim xong nhưng SFN không chạy được | TRIG# kẹt `QUEUED` | Manifest không tự chạy lại; cần `force` |
| 27 | Chạy tay trên máy UTC+7 | Session timezone ghim UTC | Cùng kết quả như trên Glue |

---

## 13. Điểm mạnh

| Điểm mạnh | Vì sao quan trọng |
|---|---|
| Thêm bảng bằng YAML, kiểm tra lúc nạp | 11 loại lỗi entry bị chặn trước khi tốn một phút Glue |
| Raw nguyên văn, schema ở silver | Nguồn thêm trường không làm job chết; envelope (SEC `tag`, GLEIF `included`) không bị mất |
| Trigger bằng manifest | Một event mỗi lần chạy, đúng danh sách file, không đọc lần chạy dở |
| Mọi writer idempotent | Retry ở bất kỳ tầng nào đều an toàn |
| SCD2 một câu MERGE | Người đọc không thấy trạng thái nửa vời (trừ khi rollback) |
| `try_cast` + đếm lỗi + ngưỡng | Một giá trị hỏng không làm mất 27.833 dòng còn lại, nhưng lỗi hàng loạt vẫn bị chặn |
| Cột kỹ thuật thống nhất | Mọi bảng truy ngược được về manifest, lần chạy, thứ tự CDC |
| Tiến hoá schema an toàn | Thêm cột tự động; đổi kiểu phải có chủ đích |
| Test trên Spark + Iceberg thật | Bắt được 5 bug mà test giả lập không thể thấy |
| Dùng chung orchestrator với sourcing | Một cách kích, một nơi xem trạng thái, thêm gold không sửa điều phối |

---

## 14. Điểm yếu và rủi ro

| Mức | Vấn đề | Chi tiết |
|---|---|---|
| **Cao** | Thứ tự trong một bảng chỉ là best-effort | Tuần tự nhờ `max_concurrent_runs = 1` + retry, nhưng retry không giữ thứ tự đến. SCD2 được bảo vệ bằng `OutOfOrderError`, đổi lại phải xử lý tay khi xảy ra |
| **Cao** | Nhóm SSI không chạy được trên AWS | WAF chặn IP datacenter; bảng chứng khoán VN (ISIN) phụ thuộc chạy sourcing từ máy |
| **Cao** | Chưa apply, chưa chạy trên Glue | Chưa kiểm IAM thật, Glue Catalog thật, `--conf` Iceberg thật cho silver |
| Trung bình | Chưa có bảo trì Iceberg | Merge-on-read tích tụ delete file và snapshot; đọc chậm dần, tốn S3 |
| Trung bình | Rollback SCD2 không nguyên tử | DELETE, UPDATE, MERGE là ba commit |
| Trung bình | Backfill SCD4a làm status các ngày sau cũ | Phải chạy lại tuần tự; chưa có lệnh tự động |
| Trung bình | Nhiều file raw nhỏ, gzip không chia được | Mỗi trang một file ~290 KB; nguồn lớn sẽ chậm và tốn chi phí liệt kê |
| Trung bình | Hai luồng kích silver cùng tồn tại | Tốn tài nguyên, khó hiểu khi điều tra; TRIG# che được trùng lặp nhưng không che được độ phức tạp |
| Trung bình | Không phát hiện đổi tên trường | Cột thành NULL im lặng nếu không có `not_null` |
| Thấp | TRIG# kẹt `QUEUED` | Chỉ FAILED được giành lại |
| Thấp | Silver không báo luồng sau | Chưa ghi manifest vào `ORCH#`; gold chưa kích tự động được |
| Thấp | Chi phí Glue ETL | Tối thiểu 2 DPU; bảng nhỏ chạy vài chục giây vẫn trả tiền phút tối thiểu |
| Thấp | Nút công ty mẹ nằm ở hai bảng | JSON:API không lặp trong `included` bản ghi đã có ở `data`; ontology phải lấy hợp |
| Thấp | CI chưa chạy test Spark; prd chưa có module | |

---

## 15. Việc cần sửa

| # | Việc | Vì sao | Mức |
|---|---|---|---|
| 1 | `terraform plan`/`apply` dev, chạy `slv-ddl-apply`, rồi event 05 (src chain → silver) và 06 | Kiểm IAM, catalog, Iceberg thật | **tiếp theo** |
| 2 | Xoá `hs_err_pid27404.log` khỏi repo, thêm `hs_err_pid*.log` vào `.gitignore` | Log JVM crash lỡ commit | ngay |
| 3 | Job bảo trì Iceberg hằng đêm: `rewrite_data_files`, `rewrite_position_delete_files`, `expire_snapshots`, `remove_orphan_files` | Merge-on-read cần compaction | trước khi bật lịch |
| 4 | Gỡ luồng phụ S3 event (rule, 2 SQS, router, Pipes, `slv-runner`) sau khi luồng chính chạy ổn; nếu cần file bên ngoài thì cho rule S3 gọi thẳng Lambda trigger | Một đường kích duy nhất | sau 1 |
| 5 | TRIG# `QUEUED` quá N giờ được giành lại; SFN RecordFailure đặt TRIG# = FAILED | Không để manifest kẹt | cải tiến |
| 6 | Tuần tự chặt cho SCD2: Lambda trigger chỉ kích manifest kế tiếp khi TRIG# trước của cùng bảng đã xong, hoặc lease theo bảng trong DynamoDB | Loại bỏ `OutOfOrderError` do retry | nếu nguồn hay lệch thứ tự |
| 7 | Lệnh backfill tuần tự: nhận khoảng ngày, kích lần lượt, dừng khi lỗi | Backfill SCD2/SCD4a đúng thứ tự | cải tiến |
| 8 | Sourcing gộp file tới ~128 MB | Giảm file nhỏ | trước nguồn lớn |
| 9 | Cảnh báo cột "gần như toàn NULL" (tỷ lệ NULL tăng đột biến so với lần trước) | Bắt đổi tên trường ở nguồn | cải tiến |
| 10 | Glue `execution_class = "FLEX"` cho bảng không gấp | Rẻ hơn khoảng 34% | cân nhắc |
| 11 | Silver ghi `manifest`/snapshot vào `ORCH#`, thêm `JobType` gold | Chain silver → gold | khi có gold |
| 12 | CI: job riêng chạy `tests/silver` với Java + pyspark | Test Spark chưa chạy trên PR | cải tiến |
| 13 | Nối `envs/prd`; quyền Lake Formation cho role `slv-glue` khi bật LF (tuần 8) | | khi cần |
| 14 | Alarm SFN `ExecutionsFailed` cho execution `slv-*`, và RUN# `warnings` lặp lại | | cải tiến |
| 15 | Mở rộng entry: `ssi_exchange_*` → bảng chứng khoán VN, `binance_exchange_info`, `okx_instruments_*`, `fdic_history` (cạnh sáp nhập) | Đủ nút và cạnh cho ontology | theo lộ trình |

---

## 16. Quyết định thiết kế

| Quyết định | Vì sao | Cái giá |
|---|---|---|
| Framework Spark tự viết, không dùng dbt-athena | Cần đọc NDJSON lồng nhau, SCD2 bằng MERGE Iceberg, idempotent theo manifest; một entry YAML sinh cả job lẫn bảng | Tự bảo trì writer; không có lineage/docs sẵn như dbt |
| Bỏ bronze, `landing/` làm bronze | Raw đã bất biến, nguyên văn, có partition | Truy vấn raw phải qua Spark, không có bảng Athena |
| Một Glue job mỗi entry | `max_concurrent_runs = 1` làm khoá theo bảng; worker theo entry | Nhiều resource; Glue ETL tối thiểu 2 DPU |
| Trigger bằng manifest, không bằng file | Một event mỗi lần chạy, đúng danh sách file | Sourcing phải ghi manifest sau cùng |
| Đọc dòng text, sinh schema từ path | Không suy schema trên dữ liệu lớn và lệch cấu trúc | Chỉ lấy được những gì entry khai |
| `try_cast` + ngưỡng thay vì fail ngay | Một giá trị hỏng không mất cả lô | Phải đặt ngưỡng hợp lý |
| SCD2 theo ngày (`date`), không theo timestamp | Khớp nhịp chạy hằng ngày; truy vấn "tại ngày D" đơn giản | Nhiều thay đổi trong một ngày chỉ giữ bản cuối |
| Từ chối dữ liệu SCD2 đến trễ | Không viết lại lịch sử ngầm | Cần thao tác tay khi xảy ra |
| SCD4a lưu mỗi ngày một ảnh đầy đủ | Truy vấn "khác hôm qua gì" không cần join | Dung lượng: FDIC ~10 triệu dòng/năm (nhỏ khi nén Parquet) |
| Merge-on-read mặc định | Ghi nhỏ nhanh | Cần compaction |
| Ghim UTC ở cả `--conf` lẫn code | Kết quả không phụ thuộc máy chạy | |
| Dùng lại `sourcing.state` | Một thiết kế bảng log cho mọi tầng | Zip silver phải kèm `sourcing/` |
