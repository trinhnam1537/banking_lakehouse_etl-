# IBM AML — chuyển tiền liên ngân hàng có nhãn rửa tiền

Bộ dữ liệu tổng hợp của IBM (Altman và cộng sự, *Realistic Synthetic Financial Transactions for
Anti-Money Laundering Models*, NeurIPS 2023 Datasets & Benchmarks). Bộ này mô phỏng đủ ba giai
đoạn rửa tiền (placement, layering, integration), có **nhãn từng giao dịch** và **file liệt kê từng
vụ theo kiểu hình**. Đây là nguồn giá trị nhất cho tầng đồ thị và ontology.

| | |
|---|---|
| Loại | Luồng giao dịch (Kafka) + file tĩnh + nhãn trễ |
| Giấy phép | CDLA-Sharing 1.0 (Kaggle `ealtman2019/ibm-transactions-for-anti-money-laundering-aml`) |
| Entry | `aml_transfers` (kafka), `aml_accounts`, `aml_patterns`, `aml_labels` (s3file) |
| Dataset raw | `landing/aml/{transfers, accounts, patterns, labels}/` |
| Profile | `aml_transfers`: `src-vpc` (**tắt**, cần Kafka). Ba entry s3file: `src-shell` (bật) |
| Code | `simulators/aml/replay.py` (`synth`, `static`, `stream`) |

## 1. Các biến thể

| Bản | Giao dịch | Tỷ lệ rửa tiền | Ghi chú |
|---|---|---|---|
| **HI-Small** | ~5,08 triệu | ~1/981 | **Khuyến nghị**: 10 ngày (01–10/09/2022), ~515 nghìn tài khoản |
| LI-Small | ~6,9 triệu | ~1/1.942 | |
| HI-Medium / LI-Medium | ~32 / 31 triệu | | |
| HI-Large / LI-Large | ~180 / 176 triệu | | ~16 GB CSV. **Không hợp** với Glue Python Shell và Kafka trên một máy spot |
| `SYN` (tổng hợp của repo) | tuỳ `--transactions` | ~1/981 | `replay synth` sinh **đúng định dạng file Kaggle** |

HI = tỷ lệ rửa tiền cao, LI = thấp. Phần đuôi của các bản Small, theo ghi nhận của cộng đồng, gần
như chỉ còn giao dịch rửa tiền (phần dở dang của các vụ). Cần kiểm tra phân bố theo ngày trước khi
dùng để đánh giá.

```powershell
kaggle datasets download -d ealtman2019/ibm-transactions-for-anti-money-laundering-aml -p data/ibm-aml --unzip
# -> data/ibm-aml/HI-Small_Trans.csv, HI-Small_accounts.csv, HI-Small_Patterns.txt
```

## 2. Schema file gốc

### `*_Trans.csv`

```
Timestamp,From Bank,Account,To Bank,Account,Amount Received,Receiving Currency,Amount Paid,Payment Currency,Payment Format,Is Laundering
2022/09/01 00:20,010,8000EBD30,010,8000EBD30,3697.34,US Dollar,3697.34,US Dollar,Reinvestment,0
```

- **Có HAI cột tên `Account`** (nguồn và đích). Đọc theo tên cột thì cột sau ghi đè cột trước,
  mất tài khoản nguồn mà không báo lỗi. Reader `s3file` giờ **chặn** header trùng tên, còn
  producer đọc theo **vị trí** cột.
- `Timestamp` có độ phân giải phút, không có múi giờ.
- 15 đồng tiền: US Dollar, Euro, Yuan, Yen, Rupee, Ruble, UK Pound, Canadian Dollar, Australian
  Dollar, Swiss Franc, Mexican Peso, Brazil Real, Saudi Riyal, Shekel, Bitcoin. **VCB không niêm yết
  MXN, BRL, ILS, BTC**. Khi tiền trả khác tiền nhận, `Amount Paid / Amount Received` chính là tỷ giá
  ngầm có sẵn trong dữ liệu.
- `Payment Format`: Reinvestment, Cheque, Credit Card, ACH, Cash, Wire, Bitcoin.
- Nhiều dòng tự chuyển (nguồn = đích, `Reinvestment`) sẽ thành self-loop trong đồ thị.

### `*_accounts.csv`

`Bank Name, Bank ID, Account Number, Entity ID, Entity Name`: tài khoản → pháp nhân. Tên pháp nhân
là nhãn tổng hợp (`Corporation #33520`, `Sole Proprietorship #...`), không phải tên thật.

### `*_Patterns.txt`

```
BEGIN LAUNDERING ATTEMPT - FAN-OUT:  Max 16-degree Fan-Out
2022/09/01 05:14,00952,8139F54E0,0111632,8062C56E0,5331.44,US Dollar,5331.44,US Dollar,ACH,1
...
END LAUNDERING ATTEMPT - FAN-OUT
```

8 kiểu hình: FAN-OUT, FAN-IN, CYCLE, SCATTER-GATHER, GATHER-SCATTER, BIPARTITE, STACK, RANDOM.
Generator tổng hợp sinh 5 kiểu đầu.

---

## 3. Luồng giả lập (`simulators/aml/replay.py`)

| Lệnh | Việc |
|---|---|
| `synth --transactions N [--entities --banks --days --laundering-ratio]` | Sinh bộ `SYN` (giao dịch thường theo phân phối Pareto, chèn các vụ rửa tiền) vào `data/ibm-aml-synthetic/` |
| `static --dataset HI-Small` | `accounts.csv` (đổi tên cột sang snake_case) và `Patterns.txt` (chuyển thành NDJSON) đẩy vào `landing/_inbox/aml/{accounts,patterns}/dt=<SIM_EPOCH>/` |
| `stream --dataset HI-Small [--speed S] [--label-delay-days 14] [--limit N]` | Phát giao dịch vào Kafka, nhãn ghi thành file trễ |

Producer đóng vai "hệ thống chuyển tiền liên ngân hàng":

| Việc | Lý do |
|---|---|
| Đổi tên cột, đọc theo vị trí | Hai cột `Account` |
| `txn_id` = sha1(10 cột nội dung + số lần xuất hiện thứ n)[:20] | File gốc không có mã giao dịch. Mã tất định, chạy lại cho mã giống hệt, nối được với nhãn và file mẫu |
| Dời thời gian để ngày đầu = `SIM_EPOCH`, giữ khoảng cách | Trục thời gian chung |
| `--speed 600`: 1 giây thật = 10 phút nghiệp vụ. `0`: nhanh nhất | HI-Small ~5 triệu / 10 ngày ≈ 6 giao dịch/giây ở tốc độ thật |
| **Key Kafka = `from_account`** | Mọi giao dịch của một tài khoản vào cùng partition, giữ thứ tự theo tài khoản |
| Producer idempotent, `acks=all` | Thử lại không sinh bản trùng |
| **Không gửi `Is Laundering`** | Nhãn đến sau 14 ngày nghiệp vụ, ghi ở `landing/_inbox/aml/labels/` |

## 4. Topic và file đích

### Kafka `aml.interbank.transfers`

6 partition, retention 14 ngày (local). Value JSON:

| Trường | Ví dụ | Ghi chú |
|---|---|---|
| `txn_id` | `559e3dbe6ea1b584429b` | Khoá nghiệp vụ |
| `event_ts` | `2023-01-01T00:01:00Z` | Đã dời về trục chung |
| `source_ts` | `2022/09/01 00:01` | Thời gian gốc |
| `from_bank`, `from_account`, `to_bank`, `to_account` | `39773`, `C1899361F` | |
| `amount_received`, `receiving_currency`, `amount_paid`, `payment_currency` | `652.87`, `US Dollar` | Chuỗi |
| `payment_format` | `Credit Card` | |

### File inbox

| Prefix | Định dạng | Cột |
|---|---|---|
| `aml/accounts/dt=…/` | CSV gzip | `bank_name, bank_id, account_number, entity_id, entity_name` |
| `aml/patterns/dt=…/` | NDJSON gzip | `attempt_id, typology, typology_detail, txn_id`, cộng 10 cột giao dịch |
| `aml/labels/dt=…/` | CSV gzip | `txn_id, is_laundering` |

`txn_id` trong file mẫu lấy theo lần xuất hiện thứ nhất. Giao dịch trong một vụ gần như luôn duy
nhất theo nội dung, nhưng về lý thuyết vẫn có thể lệch nếu một giao dịch giống hệt xuất hiện trước
đó trong cùng phút.

---

## 5. Entry sourcing

| Entry | Reader | CDC / mốc | Ghi chú |
|---|---|---|---|
| `aml_transfers` | `kafka` | CDC1, offset **từng partition** | `max_records_per_run` 500.000. HI-Small cần ~10 lần chạy. Group `fingraph-src-aml`, không commit offset vào Kafka (mốc nằm ở DynamoDB) |
| `aml_accounts` | `s3file` csv | CDC1, khoá S3 | |
| `aml_patterns` | `s3file` ndjson | CDC1, khoá S3 | Ground truth cho đồ thị |
| `aml_labels` | `s3file` csv | CDC1, khoá S3 | |

Raw của Kafka giữ nguyên bản tin, `value` là **chuỗi JSON**, silver bóc ra:

```json
{"_kafka_topic": "aml.interbank.transfers", "_kafka_partition": 0, "_kafka_offset": 0,
 "_kafka_timestamp_ms": 1791906668791, "_kafka_key": "C1899361F",
 "value": "{\"txn_id\": \"559e3dbe6ea1b584429b\", \"event_ts\": \"2023-01-01T00:01:00Z\", ...}",
 "_src_job": "aml_transfers", "_src_type": "kafka", "_src_cdc": "cdc1", "_src_ingest_ts": "..."}
```

## 6. Chất lượng dữ liệu: việc của silver

| Vấn đề | Xử lý |
|---|---|
| Self-transfer (`Reinvestment`) | Gắn cờ, loại khỏi cạnh đồ thị chuyển tiền |
| Đa tiền tệ, 4 đồng không có ở VCB | Ưu tiên tỷ giá VCB theo ngày. Thiếu thì dùng tỷ giá ngầm `amount_paid / amount_received` |
| Bitcoin | Giữ đơn vị gốc, không quy đổi VND |
| Đuôi ngày cuối lệch tỷ lệ rửa tiền | Cắt hoặc ghi chú khi đánh giá mô hình |
| Trùng do at-least-once | Khử theo `txn_id` |
| Nhãn đến sau giao dịch 14 ngày | Join nhãn khi huấn luyện, **không** dùng làm đặc trưng thời gian thực |
