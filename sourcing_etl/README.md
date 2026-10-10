# sourcing_etl — cấu hình job

Thư mục này chứa **cấu hình**, không chứa code. Mỗi file YAML trong
`sources/` là một job kéo dữ liệu.

```
sourcing/          code framework  — sua khi them LOAI nguon moi
sourcing_etl/      entry cau hinh  — sua khi them MOT nguon cu the
```

---

## Bố cục: một thư mục cho mỗi nguồn

Một nguồn dữ liệu thường có nhiều bảng — SEC có `frames`, `companyfacts`,
`submissions`; SSI có từng sàn và từng rổ. Mỗi bảng là **một job riêng**, vì
chúng khác nhau về nhịp chạy, kích thước và khả năng hỏng.

```
sources/
  sec/           6 entry    gleif/      6 entry
  ssi/           7 entry    binance/    2 entry
  okx/           5 entry    coingecko/  7 entry
  treasury/      4 entry    fdic/       6 entry
  comtrade/      6 entry    alphavantage/ 7 entry (tat, cho khoa)
  kafka_trades.yml  market_files_s3.yml  pg_instruments.yml   <- entry le
```

**Tên file phải duy nhất trên toàn bộ cây**, không chỉ trong một thư mục.
Terraform khoá theo `basename(f)` vì tên job Glue và khoá DynamoDB không
chứa được dấu `/`. Hai file trùng tên ở hai thư mục sẽ đè lên nhau mà
Terraform không báo lỗi — nên cả `load_all` lẫn
`terraform_data.unique_entry_names` đều chặn việc này lúc plan.

Quy ước: đặt tên `<nguồn>_<bảng>.yml`, và khoá `name` bên trong **trùng tên
file**. `load_all` bắt lỗi nếu lệch, vì Terraform lấy tên job từ tên file
còn framework lúc chạy lấy từ khoá `name`.

---

## Thêm một nguồn mới

```
1. Tao sources/<nguon>/<ten_job>.yml
2. terraform plan      -> thay 1 Glue job + 1 dong job_config
3. terraform apply
```

Không viết dòng Python nào.

Thử phần đọc nguồn trước khi apply — không cần AWS:

```powershell
python -c @'
from sourcing.config import load_config
from sourcing.readers.api import ApiReader
cfg = load_config("sourcing_etl/sources/ssi/ssi_group_vn30.yml")
for b in ApiReader(cfg).read(None):
    print(len(b.records), "ban ghi |", list(b.records[0])[:6]); break
'@
```

---

## Entry hiện có

59 entry, 10 nguồn API cộng ba entry lẻ. Ngày 09-27 đã bỏ 12 entry trùng lặp hoặc không có giá trị
(xem `docs/SETUP-LOG.md` mục "Rà soát giá trị nguồn").

| Thư mục | Nguồn | Cần khoá | Ghi chú |
|---|---|---|---|
| `sec/` | SEC EDGAR | không (cần User-Agent) | `frames_*` trả mảng phẳng, dùng được ngay |
| `gleif/` | GLEIF | không | pháp nhân + **công ty mẹ** (`include=direct-parent,ultimate-parent`), nguồn cạnh sở hữu cho ontology |
| `ssi/` | SSI iBoard | không (cần `Referer`) | **danh mục cổ phiếu VN kèm ISIN** |
| `binance/` `okx/` | Hai sàn crypto | không | để đối chứng giá giữa hai sàn |
| `coingecko/` | CoinGecko | không | giá tổng hợp + vốn hoá |
| `treasury/` | US Treasury | không | nợ công, tỷ giá, đấu thầu TP |
| `fdic/` | FDIC BankFind | không | ~27.800 ngân hàng Mỹ + lịch sử sáp nhập |
| `comtrade/` | UN Comtrade | không | XNK Việt Nam 2021–2023 |
| `alphavantage/` | Alpha Vantage | **có** | `enabled: false` cho tới khi có khoá |

Ba entry lẻ ở gốc: `kafka_trades` (cdc1, 5 phút), `market_files_s3`
(cdc1, 15 phút), `pg_instruments` (**tắt** — chưa có RDS). Lịch của hai entry
đầu **đang tắt** từ 09-27, bật lại khi Glue chạy được.

**Mọi entry đang để `schedule.enabled: false`.** Job
được tạo nhưng không tự chạy — bật từng cái khi đã xem dữ liệu và chốt nhịp.
Glue job không tốn tiền khi không chạy.

Vài nguồn nên chuyển sang cdc1 khi đã ổn định: nhóm `treasury/` có cột
`record_date` rất đáng tin, kéo incremental rẻ hơn nhiều.

---

## Hai chế độ CDC

| | CDC1 | CDC5 |
|---|---|---|
| Kéo gì | Chỉ phần mới hơn mốc | Toàn bộ, mỗi lần |
| Ghi raw | Thêm vào | Ghi đè partition hôm nay |
| Cần gì ở nguồn | Một cột mốc đáng tin | Không cần gì |
| Hợp với | Bảng giao dịch lớn, Kafka | Bảng tham chiếu nhỏ, tài liệu JSON |

**Chọn sai là lỗi im lặng nguy hiểm nhất ở tầng này.** CDC1 trên nguồn bị sửa
hồi tố sẽ bỏ sót thay đổi mà **không báo lỗi gì** — dữ liệu cứ thế sai dần.
Phân vân thì chọn CDC5: tốn hơn nhưng không bao giờ sót.

Nguồn vừa lớn vừa bị sửa hồi tố thì dùng CDC1 kèm `lookback_seconds` để kéo
chồng lấn. Cái giá là bản ghi trùng, bronze phải khử.

---

## Khung một entry

```yaml
name: ten_job                    # trung ten file
source_type: api                 # api | jdbc | kafka | s3file
description: Mo ta ngan
owner: namth
enabled: true                    # false = giu trong repo, khong tao job

api:                             # khoi rieng theo source_type
  base_url: https://...
  records_path: data             # duong toi mang ban ghi trong JSON
  pagination: page               # none | page | cursor | offset

cdc:
  mode: cdc1
  watermark_column: updated_at   # BAT BUOC voi cdc1 (tru kafka)
  lookback_seconds: 600          # keo chong lan, bat ban ghi den muon

target:
  dataset: nhom/ten              # -> landing/nhom/ten/ingest_date=YYYY-MM-DD/

schedule:
  enabled: true
  expression: "rate(5 minutes)"

runtime:
  engine: python_shell           # python_shell (re) | spark (dat)
  timeout_minutes: 30
  additional_python_modules:
    - confluent-kafka==2.5.3
```

---

## Ba điều dễ làm sai

**Đừng mặc định dùng Spark.** `python_shell` là 0.0625 DPU (~0.03 USD/giờ),
Spark tối thiểu 2 DPU. Một job kéo 500 bản ghi từ REST API chạy bằng Spark
là đốt tiền vô ích.

**Đừng để khoá bí mật trong YAML.** Khai tên secret và chỗ cần chèn:

```yaml
api:
  secret_name: fingraph/finnhub
  secret_inject:
    token: query.token           # secret["token"] -> ?token=...
    api_key: header.X-Api-Key    # secret["api_key"] -> header
```

**Dùng biến môi trường cho giá trị đổi theo lần apply.** IP riêng của EC2
Kafka đổi mỗi lần máy dựng lại:

```yaml
kafka:
  bootstrap_servers: "${KAFKA_BOOTSTRAP}"
```

Terraform truyền giá trị qua tham số job, framework đẩy vào `os.environ`
trước khi nạp cấu hình. Cần vậy vì Glue không cho đặt biến môi trường tuỳ ý.

---

## Lịch chạy hoạt động thế nào

Chỉ có **một** lịch EventBridge cho tất cả job:

```
EventBridge (5 phut/lan)
  -> SQS dispatch
    -> Lambda: quet job_config, loc job den han
      -> SQS run (N message)
        -> EventBridge Pipes
          -> Step Functions: chay Glue job, ghi job_log
```

Lambda so `schedule.expression` với `last_dispatched_at` để quyết định job
nào đến hạn. Thêm job không cần tạo lịch mới.

Hỗ trợ `rate(N minutes|hours|days)` và cron hằng ngày `cron(M H * * ? *)`.
Cron phức tạp hơn thì job đó cần lịch EventBridge riêng.

---

## Chạy lại một job ngay, không đợi lịch

```powershell
aws sqs send-message `
  --queue-url (terraform output -raw sourcing_dispatch_queue) `
  --message-body '{\"jobs\":[\"gleif_lei\"]}' `
  --message-group-id manual `
  --profile fingraph
```

---

## Cấu hình được lưu ở đâu

| Nơi | Vai trò |
|---|---|
| `sources/*.yml` trong git | **Nguồn chân lý** — có review, có lịch sử |
| DynamoDB `fingraph-dev-src-job-config` | Bản đọc lúc chạy, Terraform seed mỗi lần apply |

Đừng sửa item DynamoDB bằng tay: lần apply sau sẽ ghi đè, và thay đổi đó
không có trong git nên không ai review được.
