# silver_etl — cấu hình raw → silver

Code ở [`../silver/`](../silver/). Ở đây chỉ có cấu hình:

```
entries/<nguon>/<bang>.yml   mot entry = mot bang silver (chay tren Glue job dung chung slv-spark)
ddl/<database>/<bang>.sql    (tuy chon) DDL viet tay; khong co thi sinh tu entry
```

**Hiện chưa có entry silver nào**: nguồn tài chính đã bỏ (10-12), entry cho Berka viết sau.
Ví dụ bên dưới (GLEIF, FDIC) giữ lại để minh hoạ cú pháp.

**Thêm một bảng silver:** tạo `entries/<nguon>/<bang>.yml` → merge `main` (CI config-sync ghi vào `slv-job-config`).
Không viết Python. Tên file phải duy nhất và trùng khoá `name`.

---

## Entry — đủ các khoá

```yaml
name: gleif_legal_entity
description: ...
owner: namth
enabled: true

source:
  dataset: gleif/lei_records     # BAT BUOC — khop `dataset` cua manifest sourcing
  records_path: data             # mang ban ghi trong _response; mac dinh lay tu dong raw
  shape: api_response            # api_response | records | kafka_message; mac dinh theo manifest
  filter: "record_type = 'x'"    # Spark SQL tren cot DA ep kieu
  read: manifest                 # manifest (mac dinh) | partition (backfill ca ngay)

target:
  table: gleif_legal_entity      # BAT BUOC
  database: track_a_silver
  ddl: track_a_silver/x.sql      # co = tao bang tu file; khong = sinh tu columns
  partition_by: [ds_partition_date]
  properties: {write.merge.mode: copy-on-write}
  schema_evolution: add_columns  # add_columns | strict

columns:                         # BAT BUOC
  - name: lei                    # ten cot trong bang
    path: id                     # duong cham trong nguon (mac dinh = name); ten co '-' duoc
    from: record                 # record | response (envelope) | request | line
    type: string                 # string int bigint double decimal(p,s) boolean date timestamp array<..> map<..> struct<..>
    format: MM/dd/yyyy           # date/timestamp: mau Java | epoch_ms | epoch_s
    transform: "upper({col})"    # bien doi CHUOI truoc khi ep kieu
    default: "N/A"
    nullable: false
    comment: "..."
    keep: false                  # cot trung gian: tinh cot khac xong thi bo
    hash: false                  # khong tinh vao ds_hash (vd. ngay xuat ban doi moi ngay)
  - name: notional
    type: "decimal(38,8)"
    expr: "price * qty"          # cot TINH, sau khi ep kieu

technical:
  keys: [lei]                    # -> ds_key; bat buoc voi scd2/scd4a
  partition_date: ingest_date    # ingest_date | run_date | {source: column, column: x} | {source: expr, expr: ...}
  cdc_index: last_update_ts      # thu tu chon ban moi nhat; mac dinh _src_ingest_ts
  delete_when: "op = 'D'"        # nguon CDC co cot lenh xoa

writer:
  type: scd2                     # scd2 | scd4a | simple
  mode: append                   # simple: append | overwrite_partition | overwrite_table
  snapshot: true                 # scd2: nguon la anh chup day du -> key vang mat = xoa
  emit_deletes: true             # scd4a: ghi dong D cho key bien mat

quality:
  not_null: [lei]
  max_cast_error_ratio: 0.0      # ti le toi da gia tri KHONG ep duoc kieu
  duplicate_keys: warn           # warn | fail (trung key trong lo)
  min_rows: 0
  on_fail: fail                  # fail | warn

runtime:
  worker_type: G.1X
  number_of_workers: 2
  timeout_minutes: 60
```

## Cột kỹ thuật — tự sinh, không khai

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `ds_key` | string | SHA-256 của `technical.keys` |
| `ds_hash` | string | SHA-256 các cột nghiệp vụ (trừ `hash: false`) — phát hiện thay đổi |
| `ds_record_status` | string | `I` mới, `U` đổi, `D` xoá, `N` không đổi (scd4a) |
| `ds_partition_date` | date | ngày dữ liệu, theo `technical.partition_date` |
| `ds_cdc_index` | bigint | thứ tự CDC (micro-giây hoặc số) |
| `ds_etl_timestamp` | timestamp | lúc ghi |
| `ds_etl_job_id` | string | run_id |
| `ds_source` | string | manifest sinh ra dòng này — append idempotent nhờ cột này |
| `ds_valid_from` / `ds_valid_to` / `ds_is_current` | date/date/bool | **chỉ SCD2** |

## Chọn writer

| Dữ liệu | Writer | Ví dụ |
|---|---|---|
| Thực thể đổi chậm, cần lịch sử từng lần đổi | `scd2` | pháp nhân GLEIF |
| Ảnh chụp đầy đủ mỗi ngày, cần "hôm nay khác hôm qua gì" | `scd4a` | ngân hàng FDIC |
| Sự kiện / chuỗi thời gian | `simple` + `append` | ticker, lệnh khớp, frames SEC |
| Ảnh chụp theo ngày, không cần I/U/D | `simple` + `overwrite_partition` | công ty mẹ GLEIF |
| Bảng tham chiếu nhỏ | `simple` + `overwrite_table` | mã quốc gia, loại hình pháp lý |

## Chạy

```powershell
python -m silver.ddl generate                                # in DDL moi entry
python -m silver.ddl generate --out silver_etl/ddl --only X  # ghi file DDL de sua tay
pytest tests/silver                                          # 14 test Spark that can pyspark + Java

# Tren AWS
aws glue start-job-run --job-name fingraph-dev-slv-ddl-apply                 # tao bang tu thu muc ddl/
aws lambda invoke --function-name fingraph-dev-trigger --cli-binary-format raw-in-base64-out `
  --payload '{\"job_id\":\"slv.<bang>\",\"data_date\":\"2026-10-12\"}' out.json   # chay lai / backfill
```
