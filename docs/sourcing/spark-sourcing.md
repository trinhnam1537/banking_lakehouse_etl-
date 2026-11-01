# Sourcing bằng Spark: cấu hình, cỡ máy, nạp IBM AML HI-Medium

Tài liệu cho entry chạy `runtime.engine: spark` (profile `src-spark`, điểm vào `sourcing/spark_main.py`).
Python Shell vẫn là mặc định. Chỉ chuyển sang Spark khi nguồn > ~1 triệu dòng hoặc Python Shell hết RAM/giờ.
Quy tắc dung lượng và cách cắt dữ liệu lớn: [ingest-progress.md](ingest-progress.md) mục 1 và 5.

---

## 1. Kiến trúc: một Glue job, nhiều cỡ máy

```
entry YAML (runtime.size: M)
  -> Lambda trigger: Prepared.capacity = ("G.1X", 5)
  -> Step Functions: HasCapacity? -> RunGlueJobSized
       StartJobRun(JobName=fingraph-dev-src-spark, WorkerType=G.1X, NumberOfWorkers=5, Timeout=entry)
  -> sourcing.spark_main: đọc song song -> lấy mẫu/chọn cột -> ghi ndjson.gz -> mốc -> manifest
```

`StartJobRun` cho phép đè `WorkerType` và `NumberOfWorkers` **theo từng lần chạy**, nên không cần tạo Glue job
riêng cho mỗi cỡ (`src-spark-medium`, `src-spark-large`...). Một job cho ít tài nguyên Terraform hơn, IAM và log
group dùng chung, Lambda không phải biết thêm tên job. Thêm cỡ mới chỉ cần sửa `SPARK_SIZES` trong
`sourcing/config.py`.

| Cỡ | Máy | Task song song | Dùng cho | Giá (~0,44 USD/DPU-giờ) |
|---|---|---|---|---|
| (không khai) | cấu hình của job: 4 × G.1X | 12 | entry cũ | ~1,8 USD/giờ |
| `S` | 2 × G.1X | 4 | 1–5 triệu dòng, file vài trăm MB, nhãn/tài khoản | ~0,9 USD/giờ |
| `M` | 5 × G.1X | 16 | 5–50 triệu dòng, 1–5 GB đầu vào (**IBM AML HI-Medium**) | ~2,2 USD/giờ |
| `L` | 10 × G.2X | 72 | 50–200 triệu dòng, bảng rộng, có shuffle | ~8,8 USD/giờ |
| `XL` | 20 × G.2X | 152 | chỉ khi đã đo `L` thiếu | ~17,6 USD/giờ |

G.1X = 4 vCPU, 16 GB, 64 GB đĩa. G.2X = 8 vCPU, 32 GB. Một worker làm driver, nên số task = (worker − 1) × vCPU.
Glue tính tiền theo giây, tối thiểu 1 phút.

**Chọn cỡ:** bắt đầu nhỏ, đọc dòng log `doc N dong / P partition trong T s` và Spark UI (`_ops/spark-logs/sourcing/`).
Nếu số file đầu vào (hoặc số partition JDBC) **nhỏ hơn** số task thì thêm worker không nhanh hơn: phải chia
nguồn thành nhiều phần trước.

Kích tay một lần với cỡ khác (không tính là override dữ liệu, không tắt chain, không cần `dry_run`):

```json
{"job_id": "src.aml_transfers_history", "overrides": {"runtime.size": "L"}}
```

---

## 2. Tham chiếu cấu hình

### 2.1 `runtime`

```yaml
runtime:
  engine: spark
  profile: src-spark
  size: M                 # S | M | L | XL
  # worker_type: G.2X     # đè lên size (G.1X | G.2X | G.4X | G.8X)
  # number_of_workers: 8  # đè lên size, trong [2, 20] (MAX_SPARK_WORKERS chống gõ nhầm)
  timeout_minutes: 60
```

### 2.2 Khối `spark:` (tuỳ chọn, chỉ engine spark)

| Khoá | Mặc định | Ý nghĩa |
|---|---|---|
| `max_partition_mb` | 128 | `spark.sql.files.maxPartitionBytes`. Chỉ có tác dụng với file **không nén** hoặc parquet. File `.gz` luôn là một task |
| `output_files` | 0 (tự tính) | Số file raw. 0 = `ceil(số dòng / target.max_records_per_file)`. Gộp partition đã cache bằng `coalesce` (không shuffle), chỉ `repartition` khi cần nhiều file hơn |
| `sample` | không | `{keys: [...], keep_pct: N}`: lấy mẫu tất định theo `pmod(xxhash64(khoá), 10000)`. Xem 2.4 |
| `select` | tất cả | Chỉ giữ các cột này (cột `_src_*` luôn giữ) |
| `conf` | | `spark.sql.*` đặt lúc chạy, vd `spark.sql.shuffle.partitions`. Cấu hình executor thuộc Glue job, không đặt ở đây |

`spark.sql.adaptive.enabled` luôn bật.

### 2.3 `jdbc.parallel`: chia đọc song song

Khai **một** trong hai cách:

```yaml
# Cách 1: chia theo khoảng của cột số/ngày (nên có index)
parallel:
  column: trans_id
  num_partitions: 8            # cố định, HOẶC:
  # rows_per_partition: 500000 # tự tính = ceil(COUNT / 500000), tối đa max_partitions
  # max_partitions: 16         # trần số kết nối tới DB nguồn
  # lower_bound: 1             # bỏ trống -> SELECT MIN/MAX(/COUNT) trên đúng câu query đã lọc :watermark
  # upper_bound: 3682987

# Cách 2: danh sách điều kiện, mỗi điều kiện một partition (cột chuỗi, ngày phân bố lệch)
parallel:
  predicates:
    - "account_id % 4 = 0"
    - "account_id % 4 = 1"
    - "account_id % 4 = 2"
    - "account_id % 4 = 3"
```

| Khi nào | Dùng |
|---|---|
| Cột số tăng đều (id tự tăng) | `column` + `num_partitions` |
| CDC1: lần đầu rất nhiều dòng, các lần sau ít | `column` + `rows_per_partition`: lần đầu 16 kết nối, lần sau 1 |
| Cột số có khoảng trống lớn (Berka `trans_id` 1..3,6 triệu cho 1 triệu dòng) | vẫn `column`, nhưng partition sẽ lệch. Nếu một task chạy lâu gấp nhiều lần task khác, chuyển sang `predicates` |
| Cột chuỗi, hoặc muốn chia theo tháng | `predicates`. Các điều kiện phải **phủ hết và không chồng nhau**, nếu không sẽ mất hoặc trùng dòng |

Bound chỉ dùng để **chia khoảng**, không lọc dòng: dòng ngoài `[lower, upper]` rơi vào partition đầu hoặc cuối.

### 2.4 `s3file` trên Spark

| | |
|---|---|
| CDC | `cdc1` theo khoá S3 (`watermark_column: _s3_key`), hoặc `cdc5` đọc cả prefix |
| Liệt kê file | Dùng chung code với reader Python (`StartAfter` = mốc, bỏ `_SUCCESS`/`.keep`, trần `max_files_per_run`), nên đổi engine giữa chừng vẫn nối tiếp mốc |
| Mốc | Khoá file cuối của lần chạy. Mọi file chỉ có header vẫn tiến mốc |
| Định dạng | csv, ndjson, json, **parquet** (mới) |
| `column_names` | CSV: đặt tên cột theo vị trí, bỏ tên trong header (header trùng tên như file IBM AML gốc) |
| `_src_file` | Khoá S3, giống reader Python. `_src_row` là id duy nhất trong lần chạy, **không** phải số dòng trong file |

Nạp lại từ đầu: `reset_checkpoint(<entry>)`. Backfill lớn nhiều đợt: giảm `max_files_per_run`, mỗi lần chạy xử lý
một phần và tiến mốc.

### 2.5 Lấy mẫu: lưu ý tỷ lệ

Điều kiện là **mọi** khoá trong `keys` đều được giữ. Với giao dịch `keys: [from_account, to_account]`, giữ
`p`% tài khoản thì còn khoảng **p² giao dịch** (đo: 25% tài khoản → ~6% giao dịch, 50% → ~25%). Muốn giữ ~25%
giao dịch thì đặt `keep_pct: 50`. Bảng tài khoản lọc bằng `keys: [account]` với cùng `keep_pct` sẽ khớp đúng tập
đầu mút. Manifest ghi `sample` để silver/gold biết raw là tập con.

---

## 3. Runbook: IBM AML HI-Medium

Bố cục: **lịch sử đi đường file** (Spark), phần "đang diễn ra" đi Kafka như cũ. Phát 32 triệu bản tin qua Kafka ở
tốc độ thật mất khoảng 2 tháng.

| Entry | Đọc | Cỡ | Dataset raw |
|---|---|---|---|
| `aml_transfers_history` (mới) | `_inbox/aml/transfers/dt=*/HI-Medium_part-*.csv.gz`, cdc1 theo khoá | M | `aml/transfers_history` |
| `aml_labels` (chuyển sang Spark) | `_inbox/aml/labels/` | S | `aml/labels` |
| `aml_accounts` (chuyển sang Spark) | `_inbox/aml/accounts/` | S | `aml/accounts` |
| `aml_patterns` | không đổi (Python Shell) | | `aml/patterns` |
| `aml_transfers` (Kafka) | không đổi, phần sau ngày N | | `aml/transfers` |

`aml/transfers_history` là dataset riêng: bản ghi Kafka bọc `value` là chuỗi JSON, bản ghi file là cột phẳng.
Hai đường có **cùng `txn_id`** (cùng hàm `iter_trans`), silver hợp nhất và khử trùng theo `txn_id`.

```powershell
# 0. Tải bộ Kaggle (một lần)
kaggle datasets download -d ealtman2019/ibm-transactions-for-anti-money-laundering-aml -p data/ibm-aml --unzip
# -> data/ibm-aml/HI-Medium_Trans.csv (~2,9 GB), HI-Medium_accounts.csv, HI-Medium_Patterns.txt

# 1. Xem số ngày của bộ trước khi chọn --days (cột đầu là timestamp YYYY/MM/DD HH:MM)
python -c "import collections,csv; c=collections.Counter(r[0][:10] for r in csv.reader(open('data/ibm-aml/HI-Medium_Trans.csv'))); print(sorted(c.items()))"

# 2. Thử khô vài trăm nghìn dòng: không ghi S3, in tên file sẽ ghi
python -m simulators.aml.replay history --dataset HI-Medium --limit 300000 --dry

# 3. Đẩy dữ liệu tĩnh + lịch sử lên inbox (RAW_BUCKET, credential AWS như các simulator khác)
python -m simulators.aml.replay static  --dataset HI-Medium
python -m simulators.aml.replay history --dataset HI-Medium --days 14     # 1 triệu dòng/file

# 4. Kéo về raw (validate trước để thấy cỡ máy trong notes)
#    {"job_id": "src.aml_transfers_history", "action": "validate"}
#    {"job_ids": ["src.aml_accounts", "src.aml_patterns", "src.aml_transfers_history", "src.aml_labels"]}

# 5. Phần "đang diễn ra" qua Kafka, nối tiếp ngày 14
python -m simulators.aml.replay stream --dataset HI-Medium --start-day 14 --speed 600
```

Nếu bộ ít hơn 14 ngày thì bỏ `--days` (toàn bộ đi đường file) và không chạy bước 5.

**Ước tính (đo lại, ghi vào ingest-progress.md 2.2):** ~32 file vào, 32 task trên 16 task song song (2 đợt);
raw ~64 file × 500 nghìn dòng, ~1,5–2 GB `ndjson.gz`. `txn_id` dùng bộ đếm theo phút, giữ cửa sổ 1 ngày, nên RAM
của simulator không phụ thuộc kích thước file.

**HI-Large (~180 triệu):** cùng entry, thêm `spark.sample: {keys: [from_account, to_account], keep_pct: 50}`
(~25% giao dịch, ~45 triệu) và `runtime.size: L`. Lọc `aml_accounts` bằng `keys: [account_number]`, `keep_pct: 50`.

---

## 4. Đề xuất nâng cấp

Xếp theo giá trị trên công sức. Mục đã làm trong đợt này: cỡ máy theo entry, `rows_per_partition`, `predicates`,
s3file cdc1 + parquet trên Spark, lấy mẫu tất định, số file đầu ra tự tính, đường lịch sử AML.

### 4.1 Framework sourcing

| # | Đề xuất | Lợi ích | Công sức |
|---|---|---|---|
| F1 | **`runtime.execution_class: flex`** cho backfill và nguồn không gấp. `StartJobRun` nhận `ExecutionClass=FLEX` theo lần chạy, đi cùng nhánh `RunGlueJobSized` | Rẻ hơn ~34% (0,29 so với 0,44 USD/DPU-giờ). Đổi lại thời gian khởi động không đảm bảo. Kiểm lại Glue 5.0 có hỗ trợ Flex trước khi làm | Nhỏ: một trường config + một tham số SFN |
| F2 | **Bật `--enable-auto-scaling`** cho job `src-spark`. Khi đó `NumberOfWorkers` là **trần**, Glue tự trả worker thừa | Lần chạy CDC1 ít dữ liệu trên cỡ M không trả tiền 5 worker | Nhỏ: một tham số Terraform |
| F3 | **`size: auto`**: trigger chọn cỡ theo `bytes_written` và thời gian của 3 lần chạy gần nhất trong job_log | Không phải đoán cỡ, tự lớn theo nguồn | Vừa |
| F4 | **Đối soát số dòng**: JDBC đếm `COUNT(*)` (đã có khi dùng `rows_per_partition`), s3file đếm dòng mỗi file, ghi vào manifest. Silver từ chối manifest lệch | Phát hiện mất dữ liệu ngay ở raw | Vừa |
| F5 | **Phân vùng chia lệch**: `parallel.strategy: quantile` tự tính `predicates` từ phân vị của cột (một truy vấn `percentile_disc` trên nguồn) | Hết task "đuôi dài" với cột có khoảng trống như Berka `trans_id` | Vừa |
| F6 | **Raw Parquet tuỳ chọn** cho bảng rộng/rất lớn (ingest-progress.md 5.4), silver đọc theo định dạng trong manifest | Nhỏ hơn `ndjson.gz` 3–6 lần với bảng như Santander | Lớn: đổi hợp đồng raw |
| F7 | **Silver dùng chung cơ chế cỡ máy**: `silver/config.py` đã có `worker_type`, `number_of_workers` nhưng job dùng chung chưa đọc | Silver AML HI-Medium cần M/L, bảng corebank chỉ cần S | Nhỏ: `SilverType.prepare` trả `capacity` |

### 4.2 Hệ thống

| # | Đề xuất | Lợi ích |
|---|---|---|
| S1 | **Mẫu "lịch sử bằng file, hiện tại bằng luồng"** của AML áp dụng cho PaySim (6,36 triệu), Home Credit (58 triệu dòng: lịch sử từ CSV, JDBC chỉ kéo phần simulator sinh thêm) | Không đổ dữ liệu lớn qua Postgres hay Kafka chỉ để đọc ra lại |
| S2 | **Chặn chi phí**: AWS Budgets cảnh báo theo tag `pipeline=sourcing`; `MAX_SPARK_WORKERS` đã chặn ở config | Một entry `XL` chạy lặp theo lịch không âm thầm đốt tiền |
| S3 | **Dashboard từ job_log**: số dòng, MB, thời gian, cỡ máy theo lần chạy (CloudWatch metric từ Lambda kết thúc, hoặc Athena trên bản export DynamoDB) | Thấy xu hướng tăng trưởng, chọn cỡ có số liệu, nhận ra nguồn ngày nào cũng `NO_DATA` |
| S4 | **Compaction raw hằng ngày** cho nguồn micro-batch (thẻ Kafka mỗi 5 phút một file) bằng chính `src-spark` cỡ S | Ít file nhỏ, silver đọc nhanh hơn |
| S5 | **Lifecycle S3** cho `landing/` theo ingest-progress.md mục 4 | Giữ mỗi nguồn trong ngân sách 5 GB |
| S6 | **Kiểm hợp đồng file inbox trong CI**: test đọc 100 dòng đầu file do simulator sinh, so header với entry | Simulator đổi cột thì CI đỏ, không đợi Glue chết |
