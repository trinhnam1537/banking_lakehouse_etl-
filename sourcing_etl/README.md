# sourcing_etl — cấu hình job

Thư mục này chứa **cấu hình**, không chứa code. Mỗi file YAML trong `sources/` là
một job kéo dữ liệu.

```
sourcing/          code framework  — sửa khi thêm LOẠI nguồn mới
sourcing_etl/      entry cấu hình  — sửa khi thêm MỘT nguồn cụ thể
```

---

## Bố cục: một thư mục cho mỗi nguồn

Một nguồn thường có nhiều bảng, mỗi bảng là **một entry riêng** (khác nhịp chạy,
kích thước, chế độ CDC).

```
sources/
  berka/         8 entry   core banking Berka (MariaDB CTU): account, card, client,
                           disp, district, loan, order, trans
```

Phân tích nguồn: [docs/sources/berka.md](../docs/sources/berka.md).

**Tên file phải duy nhất trên toàn cây** và trùng khoá `name` bên trong. Tên này là
khoá của bảng `job_config` và là phần `<tên>` trong `job_id = src.<tên>`.
`load_all` chặn trùng tên và lệch tên ngay trong pytest. Quy ước đặt tên là
`<nguồn>_<bảng>.yml`.

---

## Thêm một nguồn mới

```
1. Tạo sources/<nguồn>/<tên>.yml
2. pytest                     -> kiểm tra entry (schema, profile, tiền tố secret)
3. PR -> merge main           -> CI deploy: config-sync ghi entry vào job_config
4. Lambda trigger {"job_id": "src.<tên>", "action": "validate"}, rồi dry_run, rồi chạy thật
```

**Không chạy Terraform, không viết Python, không tạo Glue job.** Entry chạy trên Glue
job dùng chung của profile (`runtime.profile`, mặc định `src-shell`).

Thử phần đọc nguồn trước, không cần AWS:

```powershell
.\scripts\run-source.ps1 -Job berka_order -Mode dry
```

---

## Hai chế độ CDC

| | CDC1 | CDC5 |
|---|---|---|
| Kéo gì | Chỉ phần mới hơn mốc | Toàn bộ, mỗi lần |
| Ghi raw | Thêm vào | Ghi đè partition hôm nay |
| Cần gì ở nguồn | Một cột mốc đáng tin (`updated_at`, id tăng dần, offset Kafka) | Không cần gì |
| Hợp với | Bảng giao dịch lớn, Kafka | Bảng tham chiếu nhỏ |

**Chọn sai là lỗi im lặng nguy hiểm nhất ở tầng này.** CDC1 trên nguồn bị sửa hồi tố
sẽ bỏ sót thay đổi mà **không báo lỗi gì**. Phân vân thì chọn CDC5: tốn hơn nhưng
không bao giờ sót. CDC1 cũng không thấy bản ghi bị **xoá cứng**: dùng xoá mềm, hoặc
thêm một entry CDC5 định kỳ để đối soát.

Ví dụ trong repo: 7 bảng Berka nhỏ dùng CDC5; `berka_trans` (1,06 triệu dòng) dùng CDC1
theo `trans_id`, để job dừng giữa chừng thì lần sau chạy tiếp từ ảnh chụp PARTIAL.

---

## Khung một entry

```yaml
name: ten_job                    # trùng tên file
source_type: jdbc                # api | jdbc | kafka | s3file
description: Mô tả ngắn
owner: namth
enabled: true                    # false = giữ trong repo, Lambda trả SKIPPED

jdbc:                            # khối riêng theo source_type
  secret_name: fingraph/src/ten  # BẮT BUỘC dưới fingraph/src/ — Glue chỉ đọc được tiền tố này
  driver: mysql                  # mysql | postgresql
  schema: financial
  table: trans

cdc:
  mode: cdc1
  watermark_column: trans_id     # BẮT BUỘC với cdc1 (trừ kafka)
  watermark_type: int            # timestamp | int | string
  lookback_seconds: 0            # kéo chồng lấn để bắt bản ghi đến muộn

target:
  dataset: nhom/ten              # -> landing/nhom/ten/ingest_date=YYYY-MM-DD/

schedule:
  enabled: false                 # true -> Lambda schedule-sync tạo lịch EventBridge
  expression: "cron(0 20 ? * SUN *)"   # giờ UTC; rate(N minutes|hours|days) hoặc cron 6 trường

runtime:
  profile: src-shell             # Glue job dùng chung; thư viện thuộc profile (src-shell có PyMySQL)
  timeout_minutes: 30            # truyền theo từng lần chạy
```

---

## Ba điều dễ làm sai

**Đừng để khoá bí mật trong YAML.** Khai tên secret (dưới `fingraph/src/`) và chỗ cần chèn:

```yaml
api:
  secret_name: fingraph/src/ten_nguon
  secret_inject:
    token: query.token           # secret["token"] -> ?token=...
    api_key: header.X-Api-Key    # secret["api_key"] -> header
```

Secret JDBC phải là JSON `{host, port, username, password, dbname}`. Dùng một user
**chỉ đọc** riêng cho ETL, đừng dùng secret master `rds!db-…` mà RDS tự sinh (nằm ngoài
tiền tố, Glue không đọc được).

**Cần thư viện không có trong profile** (`confluent-kafka`, `psycopg2`): thêm profile ở
repo hạ tầng **trước** (`var.profiles`), rồi thêm tên vào `PROFILES` trong
`sourcing/config.py`. Khoá `additional_python_modules` trong entry không còn tác dụng.

**Giá trị đổi theo hạ tầng** (IP Kafka…): dùng `${BIEN}` trong YAML, Glue job truyền
`--BIEN` và framework đẩy vào `os.environ` trước khi nạp cấu hình.

---

## Lịch chạy

Lịch khai ngay trong entry (`schedule`). Khi entry được ghi vào `job_config`, Lambda
**schedule-sync** (nghe DynamoDB Streams) tạo, sửa hoặc xoá lịch
`fingraph-<env>-jobs/src-<tên>` trên EventBridge Scheduler. Lịch gọi Lambda trigger với
`{"job_id": "src.<tên>"}`. Cron đầy đủ cú pháp EventBridge, **giờ UTC**.

Công tắc chung nằm ở repo hạ tầng (`use_orchestrator_schedules`). Khi tắt thì không
lịch nào được tạo.

## Chạy ngay, không đợi lịch

Gọi Lambda trigger. Event mẫu ở [orchestrator/events/](../orchestrator/events/):

```powershell
aws lambda invoke --function-name fingraph-dev-trigger --cli-binary-format raw-in-base64-out `
  --cli-read-timeout 900 --payload file://orchestrator/events/03_src_run_wait.json out.json
```

---

## Cấu hình được lưu ở đâu

| Nơi | Vai trò |
|---|---|
| `sources/**/*.yml` trong git | **Nguồn chân lý**: có review, có lịch sử |
| DynamoDB `fingraph-<env>-src-job-config` | Bản đọc lúc chạy. **config-sync** (CI repo này) ghi mỗi lần deploy |

Đừng sửa item DynamoDB bằng tay: lần deploy sau sẽ ghi đè, và thay đổi đó không có trong
git. Entry bị xoá khỏi repo thì config-sync đặt `enabled = false`, không xoá item.
